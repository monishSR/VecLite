package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	indexMarker = uint32(0xDEADBEEF) // Magic number to mark start of index
	deletedID   = ^uint64(0)         // Special ID to mark deleted vectors (tombstone) - all bits set (-1)
)

// Storage handles persistent storage of vectors and metadata
type Storage struct {
	mu          sync.RWMutex // Protects file I/O and index map
	filePath    string
	file        *os.File
	dimension   int                          // Vector dimension (stored in index metadata, not per-record)
	index       map[uint64]int64              // Index: ID -> file offset for fast lookups
	vectorCache *lru.Cache[uint64, []float32] // LRU cache for vectors
}

// NewStorage creates a new storage instance
// dimension: vector dimension (must be > 0)
// cacheCapacity: 0 = disabled, >0 = cache size (default: 1000 if < 0)
func NewStorage(filePath string, dimension int, cacheCapacity int) (*Storage, error) {
	if dimension <= 0 {
		return nil, errors.New("dimension must be greater than 0")
	}

	// Default cache capacity if negative
	if cacheCapacity < 0 {
		cacheCapacity = 1000
	}

	var cache *lru.Cache[uint64, []float32]
	if cacheCapacity > 0 {
		var err error
		cache, err = lru.New[uint64, []float32](cacheCapacity)
		if err != nil {
			return nil, fmt.Errorf("failed to create LRU cache: %w", err)
		}
	}

	return &Storage{
		filePath:    filePath,
		dimension:   dimension,
		index:       make(map[uint64]int64),
		vectorCache: cache,
	}, nil
}

// Open opens the storage file and loads the index
func (s *Storage) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var err error
	s.file, err = os.OpenFile(s.filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	// Try to load index from end of file, fallback to rebuild if not found
	if err := s.loadIndex(); err != nil {
		// If index doesn't exist or is corrupted, rebuild it
		return s.rebuildIndex()
	}

	return nil
}

// loadIndex reads the index from the end of the file
// Note: Assumes lock is already held (called from Open)
func (s *Storage) loadIndex() error {
	if s.file == nil {
		return errors.New("storage file not open")
	}

	// Get file size
	fileInfo, err := s.file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	// Need at least 4 bytes for marker
	if fileSize < 4 {
		return errors.New("file too small to contain index")
	}

	// Seek to 4 bytes before end to check for marker
	if _, err := s.file.Seek(-4, io.SeekEnd); err != nil {
		return err
	}

	var marker uint32
	if err := binary.Read(s.file, binary.LittleEndian, &marker); err != nil {
		return err
	}

	// If no marker, index doesn't exist
	if marker != indexMarker {
		return errors.New("index marker not found")
	}

	// Read index count (4 bytes before marker)
	if _, err := s.file.Seek(-8, io.SeekEnd); err != nil {
		return err
	}

	var count uint32
	if err := binary.Read(s.file, binary.LittleEndian, &count); err != nil {
		return err
	}

	// Read dimension (4 bytes before count)
	if _, err := s.file.Seek(-12, io.SeekEnd); err != nil {
		return err
	}

	var dim uint32
	if err := binary.Read(s.file, binary.LittleEndian, &dim); err != nil {
		return err
	}

	// Store dimension
	s.dimension = int(dim)

	// Calculate index start position
	// Each entry: 8 bytes (ID) + 8 bytes (offset) = 16 bytes
	// Metadata: 4 bytes (dimension) + 4 bytes (count) + 4 bytes (marker) = 12 bytes
	indexSize := int64(count * 16)
	indexStart := fileSize - 12 - indexSize // 12 bytes for dimension + count + marker

	if indexStart < 0 {
		return errors.New("invalid index size")
	}

	// Seek to index start
	if _, err := s.file.Seek(indexStart, io.SeekStart); err != nil {
		return err
	}

	// Read index entries
	s.index = make(map[uint64]int64)
	for i := uint32(0); i < count; i++ {
		var id uint64
		var offset int64
		if err := binary.Read(s.file, binary.LittleEndian, &id); err != nil {
			return err
		}
		if err := binary.Read(s.file, binary.LittleEndian, &offset); err != nil {
			return err
		}
		s.index[id] = offset
	}

	return nil
}

// saveIndex writes the index to the end of the file
// Note: Assumes lock is already held (called from Sync/Close)
func (s *Storage) saveIndex() error {
	if s.file == nil {
		return errors.New("storage file not open")
	}

	// Check if there's an existing index and truncate before it
	fileInfo, err := s.file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	// If file has index marker, truncate before it
	if fileSize >= 4 {
		if _, err := s.file.Seek(-4, io.SeekEnd); err == nil {
			var marker uint32
			if err := binary.Read(s.file, binary.LittleEndian, &marker); err == nil && marker == indexMarker {
				// Read count to find where index starts
				if _, err := s.file.Seek(-8, io.SeekEnd); err == nil {
					var count uint32
					if err := binary.Read(s.file, binary.LittleEndian, &count); err == nil {
						// New format: dimension + count + marker = 12 bytes
						indexSize := int64(count * 16)
						indexStart := fileSize - 12 - indexSize
						if indexStart > 0 {
							if err := s.file.Truncate(indexStart); err != nil {
								return err
							}
						}
					}
				}
			}
		}
	}

	// Seek to end of data
	if _, err := s.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	// Write index entries
	count := uint32(len(s.index))
	for id, offset := range s.index {
		if err := binary.Write(s.file, binary.LittleEndian, id); err != nil {
			return err
		}
		if err := binary.Write(s.file, binary.LittleEndian, offset); err != nil {
			return err
		}
	}

	// Write metadata: dimension, count, and marker
	if err := binary.Write(s.file, binary.LittleEndian, uint32(s.dimension)); err != nil {
		return err
	}
	if err := binary.Write(s.file, binary.LittleEndian, count); err != nil {
		return err
	}
	if err := binary.Write(s.file, binary.LittleEndian, indexMarker); err != nil {
		return err
	}

	return nil
}

// rebuildIndex scans the file and builds the ID -> offset index
// This is used as a fallback when loadIndex() fails (new file, corrupted index, etc.)
// Note: Assumes lock is already held (called from Open)
func (s *Storage) rebuildIndex() error {
	if s.file == nil {
		return errors.New("storage file not open")
	}

	s.index = make(map[uint64]int64)

	// Get file size to know where data ends (before any existing index)
	fileInfo, err := s.file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	// If file is empty, just return empty index
	if fileSize == 0 {
		return nil
	}

	// Check if there's an index at the end and find where data ends
	dataEnd := fileSize
	if fileSize >= 4 {
		// Check for index marker
		if _, err := s.file.Seek(-4, io.SeekEnd); err == nil {
			var marker uint32
			if err := binary.Read(s.file, binary.LittleEndian, &marker); err == nil && marker == indexMarker {
				// Index exists, find where it starts
				if _, err := s.file.Seek(-8, io.SeekEnd); err == nil {
					var count uint32
					if err := binary.Read(s.file, binary.LittleEndian, &count); err == nil {
						// Read dimension (4 bytes before count)
						if _, err := s.file.Seek(-12, io.SeekEnd); err == nil {
							var dim uint32
							if err := binary.Read(s.file, binary.LittleEndian, &dim); err == nil {
								// New format: dimension + count + marker = 12 bytes
								s.dimension = int(dim)
								indexSize := int64(count * 16)
								dataEnd = fileSize - 12 - indexSize
								if dataEnd < 0 {
									dataEnd = 0
								}
							}
						}
					}
				}
			}
		}
	}

	// Seek to beginning and scan only the data portion
	if _, err := s.file.Seek(0, 0); err != nil {
		return err
	}

	// Scan through file and build index (stop at dataEnd)
	// New format: records don't have dimension field, use dimension from metadata
	for {
		// Get current offset (where this vector starts)
		offset, err := s.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		// Stop if we've reached the end of data section
		if offset >= dataEnd {
			break
		}

		// Read ID
		var id uint64
		if err := binary.Read(s.file, binary.LittleEndian, &id); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Skip vector data (dimension is in metadata, not per-record)
		vectorSize := int64(s.dimension * 4) // float32 is 4 bytes
		if _, err := s.file.Seek(vectorSize, io.SeekCurrent); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Only index non-deleted vectors (skip tombstones)
		if id != deletedID {
			s.index[id] = offset
		}
	}

	return nil
}

// compact removes all tombstones and rewrites the file with only active vectors
// Note: Assumes lock is already held (called from Close)
func (s *Storage) compact() error {
	if s.file == nil {
		return errors.New("storage file not open")
	}

	// Read all active vectors directly (skip tombstones) - inline ReadAllVectors logic
	fileInfo, err := s.file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	// Find where data ends (before index)
	dataEnd := fileSize
	if fileSize >= 4 {
		// Check for index marker
		if _, err := s.file.Seek(-4, io.SeekEnd); err == nil {
			var marker uint32
			if err := binary.Read(s.file, binary.LittleEndian, &marker); err == nil && marker == indexMarker {
				// Index exists, find where it starts
				if _, err := s.file.Seek(-8, io.SeekEnd); err == nil {
					var count uint32
					if err := binary.Read(s.file, binary.LittleEndian, &count); err == nil {
						indexSize := int64(count * 16)     // Each entry: 8 bytes ID + 8 bytes offset
						dataEnd = fileSize - 8 - indexSize // 8 bytes for count + marker
						if dataEnd < 0 {
							dataEnd = 0
						}
					}
				}
			}
		}
	}

	// Seek to beginning and read all active vectors
	if _, err := s.file.Seek(0, 0); err != nil {
		return err
	}

	vectors := make(map[uint64][]float32)
	for {
		currentPos, err := s.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if currentPos >= dataEnd {
			break
		}

		var id uint64
		if err := binary.Read(s.file, binary.LittleEndian, &id); err != nil {
			if err == io.EOF {
				break
			}
			if len(vectors) == 0 {
				return err
			}
			break
		}

		// New format: read vector data directly (dimension from metadata)
		vector := make([]float32, s.dimension)
		if err := binary.Read(s.file, binary.LittleEndian, &vector); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Skip deleted vectors (tombstones)
		if id != deletedID {
			vectors[id] = vector
		}
	}

	// If no vectors, just truncate
	if len(vectors) == 0 {
		if err := s.file.Truncate(0); err != nil {
			return err
		}
		s.index = make(map[uint64]int64)
		// Clear cache if enabled
		if s.vectorCache != nil {
			s.vectorCache.Purge()
		}
		return nil
	}

	// Truncate file to start fresh
	if err := s.file.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate file: %w", err)
	}

	// Seek to beginning
	if _, err := s.file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek to beginning: %w", err)
	}

	// Rebuild index
	s.index = make(map[uint64]int64)

	// Clear cache if enabled
	if s.vectorCache != nil {
		s.vectorCache.Purge()
	}

	// Rewrite all active vectors directly - inline WriteVector logic
	for vecID, vector := range vectors {
		// Get current offset (where this vector will start)
		offset, err := s.file.Seek(0, io.SeekEnd)
		if err != nil {
			return fmt.Errorf("failed to rewrite vector %d: %w", vecID, err)
		}

		// Write ID (8 bytes)
		if err := binary.Write(s.file, binary.LittleEndian, vecID); err != nil {
			return fmt.Errorf("failed to rewrite vector %d: %w", vecID, err)
		}

		// Write vector data (dimension is stored in index metadata, not per-record)
		if err := binary.Write(s.file, binary.LittleEndian, vector); err != nil {
			return fmt.Errorf("failed to rewrite vector %d: %w", vecID, err)
		}

		// Update index
		s.index[vecID] = offset

		// Update cache if enabled
		if s.vectorCache != nil {
			vecCopy := make([]float32, len(vector))
			copy(vecCopy, vector)
			s.vectorCache.Add(vecID, vecCopy)
		}
	}

	return nil
}

// Close closes the storage file, compacts tombstones, and saves the index
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file != nil {
		// Compact file to remove tombstones before closing
		if err := s.compact(); err != nil {
			// Log error but still try to close
			_ = s.file.Close()
			return fmt.Errorf("failed to compact file: %w", err)
		}

		// Save index before closing
		if err := s.saveIndex(); err != nil {
			// Log error but still close file
			_ = s.file.Close()
			return fmt.Errorf("failed to save index: %w", err)
		}
		return s.file.Close()
	}
	return nil
}

// WriteVector writes a vector to storage
// Always appends to the end of the file
func (s *Storage) WriteVector(id uint64, vector []float32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return errors.New("storage file not open")
	}

	// Seek to end of file to append (get offset where this vector will start)
	offset, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// Validate dimension
	if len(vector) != s.dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", s.dimension, len(vector))
	}

	// Write ID (8 bytes)
	if err := binary.Write(s.file, binary.LittleEndian, id); err != nil {
		return err
	}

	// Write vector data (dimension is stored in index metadata, not per-record)
	if err := binary.Write(s.file, binary.LittleEndian, vector); err != nil {
		return err
	}

	// Update index
	s.index[id] = offset

	return nil
}

// getCachedVector retrieves a vector from cache if available
// Returns the vector copy and true if found, nil and false otherwise
// Thread-safe: can be called without holding the lock
func (s *Storage) getCachedVector(id uint64) ([]float32, bool) {
	if s.vectorCache == nil {
		return nil, false
	}
	vec, cached := s.vectorCache.Get(id)
	if !cached {
		return nil, false
	}
	// Return a copy to avoid external modifications
	vecCopy := make([]float32, len(vec))
	copy(vecCopy, vec)
	return vecCopy, true
}

// ReadVector reads a vector from storage by ID using the index for fast lookup
// Uses LRU cache to avoid redundant disk reads
// Optimized: checks cache before acquiring lock to allow concurrent cache hits
func (s *Storage) ReadVector(id uint64) ([]float32, error) {
	// Check cache FIRST (before locking) - cache is thread-safe
	// This allows concurrent cache hits without lock contention
	if vec, cached := s.getCachedVector(id); cached {
		return vec, nil
	}

	// Only acquire lock for cache miss (file I/O needed)
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return nil, errors.New("storage file not open")
	}

	// Double-check cache after acquiring lock (another goroutine might have added it)
	if vec, cached := s.getCachedVector(id); cached {
		return vec, nil
	}

	// Look up offset in index
	offset, exists := s.index[id]
	if !exists {
		return nil, fmt.Errorf("vector with ID %d not found", id)
	}

	// Seek to the vector's offset
	if _, err := s.file.Seek(offset, 0); err != nil {
		return nil, err
	}

	// Read ID (verify it matches)
	var vecID uint64
	if err := binary.Read(s.file, binary.LittleEndian, &vecID); err != nil {
		return nil, err
	}
	if vecID != id {
		return nil, fmt.Errorf("vector ID mismatch at offset %d: expected %d, got %d", offset, id, vecID)
	}

	// New format: read vector data directly (dimension from metadata)
	vector := make([]float32, s.dimension)
	if err := binary.Read(s.file, binary.LittleEndian, &vector); err != nil {
		return nil, err
	}

	// Cache it if cache is enabled (make a copy to avoid external modifications)
	if s.vectorCache != nil {
		vecCopy := make([]float32, len(vector))
		copy(vecCopy, vector)
		s.vectorCache.Add(id, vecCopy)
		return vecCopy, nil
	}

	return vector, nil
}

// ReadAllVectors reads all vectors from storage sequentially
// Returns a map of ID -> vector
// Stops at data boundary (before index section)
// Note: Uses Lock() instead of RLock() because os.File operations are not thread-safe
func (s *Storage) ReadAllVectors() (map[uint64][]float32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return nil, errors.New("storage file not open")
	}

	// Get file size to find data boundary
	fileInfo, err := s.file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()

	// Find where data ends (before index)
	dataEnd := fileSize
	if fileSize >= 4 {
		// Check for index marker
		if _, err := s.file.Seek(-4, io.SeekEnd); err == nil {
			var marker uint32
			if err := binary.Read(s.file, binary.LittleEndian, &marker); err == nil && marker == indexMarker {
				// Index exists, find where it starts
				if _, err := s.file.Seek(-8, io.SeekEnd); err == nil {
					var count uint32
					if err := binary.Read(s.file, binary.LittleEndian, &count); err == nil {
						// Read dimension (4 bytes before count)
						if _, err := s.file.Seek(-12, io.SeekEnd); err == nil {
							var dim uint32
							if err := binary.Read(s.file, binary.LittleEndian, &dim); err == nil {
								// New format: dimension + count + marker = 12 bytes
								s.dimension = int(dim)
								indexSize := int64(count * 16)
								dataEnd = fileSize - 12 - indexSize
								if dataEnd < 0 {
									dataEnd = 0
								}
							}
						}
					}
				}
			}
		}
	}

	// Seek to beginning
	if _, err := s.file.Seek(0, 0); err != nil {
		return nil, err
	}

	vectors := make(map[uint64][]float32)

	// Read vectors until data boundary
	for {
		// Check if we've reached data boundary
		currentPos, err := s.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		if currentPos >= dataEnd {
			break
		}

		var id uint64
		if err := binary.Read(s.file, binary.LittleEndian, &id); err != nil {
			if err == io.EOF {
				break
			}
			// For other errors, check if we've read at least one vector
			if len(vectors) == 0 {
				return nil, err
			}
			// If we've read some vectors, EOF is likely
			break
		}

		// New format: read vector data directly (dimension from metadata)
		vector := make([]float32, s.dimension)
		if err := binary.Read(s.file, binary.LittleEndian, &vector); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Skip deleted vectors (tombstones)
		if id != deletedID {
			vectors[id] = vector
		}
	}

	return vectors, nil
}

// DeleteVector marks a vector as deleted using a tombstone (ID = ^uint64(0), all bits set)
// This is much more efficient than rewriting the entire file
func (s *Storage) DeleteVector(id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return errors.New("storage file not open")
	}

	// Remove from cache if enabled
	if s.vectorCache != nil {
		s.vectorCache.Remove(id)
	}

	// Check if vector exists in index
	offset, exists := s.index[id]
	if !exists {
		return nil // Vector not found, nothing to delete
	}

	// Seek to the vector's offset
	if _, err := s.file.Seek(offset, 0); err != nil {
		return err
	}

	// Read the vector ID to verify
	var vecID uint64
	if err := binary.Read(s.file, binary.LittleEndian, &vecID); err != nil {
		return err
	}
	if vecID != id {
		return fmt.Errorf("vector ID mismatch at offset %d: expected %d, got %d", offset, id, vecID)
	}

	// Dimension is not needed for deletion - we just mark the ID as deleted

	// Seek back to the start of this vector
	if _, err := s.file.Seek(offset, 0); err != nil {
		return err
	}

	// Write tombstone: ID = ^uint64(0) (all bits set, marks as deleted)
	if err := binary.Write(s.file, binary.LittleEndian, deletedID); err != nil {
		return err
	}

	// Keep dimension and vector data (we just mark ID as deleted)
	// This way we don't need to shift anything, just skip on read

	// Remove from index
	delete(s.index, id)

	return nil
}

// Clear removes all vectors from storage
// Truncates the file and clears the index
func (s *Storage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return errors.New("storage file not open")
	}

	// Clear cache if enabled
	if s.vectorCache != nil {
		s.vectorCache.Purge()
	}

	// Truncate file to remove all data
	if err := s.file.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate file: %w", err)
	}

	// Seek to beginning
	if _, err := s.file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek to beginning: %w", err)
	}

	// Clear index
	s.index = make(map[uint64]int64)

	return nil
}

// GetFilePath returns the file path of the storage
func (s *Storage) GetFilePath() string {
	return s.filePath
}

// Sync flushes data to disk and saves the index
func (s *Storage) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file != nil {
		// Save index
		if err := s.saveIndex(); err != nil {
			return err
		}
		return s.file.Sync()
	}
	return nil
}
