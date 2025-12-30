package ivf

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// writeIVFHeader writes the IVF file header (magic, version, metadata)
func (i *IVFIndex) writeIVFHeader(w io.Writer) error {
	// Write magic number for validation
	magic := uint32(0x49564620) // "IVF " in ASCII
	if err := binary.Write(w, binary.LittleEndian, magic); err != nil {
		return fmt.Errorf("failed to write magic number: %w", err)
	}

	// Write version (for future compatibility)
	version := uint32(1)
	if err := binary.Write(w, binary.LittleEndian, version); err != nil {
		return fmt.Errorf("failed to write version: %w", err)
	}

	// Write metadata (configuration parameters and runtime state)
	// Configuration parameters
	if err := binary.Write(w, binary.LittleEndian, uint32(i.nClusters)); err != nil {
		return fmt.Errorf("failed to write nClusters: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(i.nProbe)); err != nil {
		return fmt.Errorf("failed to write nProbe: %w", err)
	}
	// Runtime state
	if err := binary.Write(w, binary.LittleEndian, uint32(len(i.centroids))); err != nil {
		return fmt.Errorf("failed to write centroid count: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(i.size)); err != nil {
		return fmt.Errorf("failed to write size: %w", err)
	}

	return nil
}

// writeCentroids writes all centroids to the writer
func (i *IVFIndex) writeCentroids(w io.Writer) error {
	for _, centroid := range i.centroids {
		// Write cluster ID
		if err := binary.Write(w, binary.LittleEndian, int32(centroid.ID)); err != nil {
			return fmt.Errorf("failed to write centroid ID %d: %w", centroid.ID, err)
		}
		// Write centroid vector ID
		if err := binary.Write(w, binary.LittleEndian, centroid.VectorID); err != nil {
			return fmt.Errorf("failed to write centroid vector ID for cluster %d: %w", centroid.ID, err)
		}
	}
	return nil
}

// writeClusterAssignments writes all cluster assignments (vectorID -> clusterID)
func (i *IVFIndex) writeClusterAssignments(w io.Writer) error {
	// Write number of assignments
	if err := binary.Write(w, binary.LittleEndian, uint32(len(i.vectorToCluster))); err != nil {
		return fmt.Errorf("failed to write assignment count: %w", err)
	}

	// Write each assignment
	for vecID, clusterID := range i.vectorToCluster {
		if err := binary.Write(w, binary.LittleEndian, vecID); err != nil {
			return fmt.Errorf("failed to write vector ID %d: %w", vecID, err)
		}
		if err := binary.Write(w, binary.LittleEndian, int32(clusterID)); err != nil {
			return fmt.Errorf("failed to write cluster ID for vector %d: %w", vecID, err)
		}
	}
	return nil
}

// SaveIVF saves the IVF structure to disk
// IVF file path is automatically derived from storage file path by appending ".ivf"
func (i *IVFIndex) SaveIVF() error {
	if i.storage == nil {
		return errors.New("storage is required to save IVF")
	}

	// Derive IVF path from storage file path
	storagePath := i.storage.GetFilePath()
	ivfPath := storagePath + ".ivf"

	file, err := os.Create(ivfPath)
	if err != nil {
		return fmt.Errorf("failed to create IVF file: %w", err)
	}
	defer file.Close()

	// Write header (magic, version, metadata)
	if err := i.writeIVFHeader(file); err != nil {
		return err
	}

	// Write centroids
	if err := i.writeCentroids(file); err != nil {
		return err
	}

	// Write cluster assignments
	if err := i.writeClusterAssignments(file); err != nil {
		return err
	}

	return nil
}

// LoadIVF loads the IVF structure from disk
// IVF file path is automatically derived from storage file path by appending ".ivf"
func (i *IVFIndex) LoadIVF() error {
	if i.storage == nil {
		return errors.New("storage is required to load IVF")
	}

	// Get dimension from storage (not from IVF file)
	i.dimension = i.storage.GetDimension()
	if i.dimension <= 0 {
		return errors.New("invalid dimension from storage")
	}

	// Derive IVF path from storage file path
	storagePath := i.storage.GetFilePath()
	ivfPath := storagePath + ".ivf"

	file, err := os.Open(ivfPath)
	if err != nil {
		return fmt.Errorf("failed to open IVF file: %w", err)
	}
	defer file.Close()

	// Read and validate magic number
	var magic uint32
	if err := binary.Read(file, binary.LittleEndian, &magic); err != nil {
		return fmt.Errorf("failed to read magic number: %w", err)
	}
	if magic != 0x49564620 { // "IVF "
		return fmt.Errorf("invalid IVF file: magic number mismatch")
	}

	// Read version
	var version uint32
	if err := binary.Read(file, binary.LittleEndian, &version); err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}
	if version != 1 {
		return fmt.Errorf("unsupported IVF file version: %d", version)
	}

	// Read metadata (configuration parameters and runtime state)
	// Configuration parameters
	var nClusters, nProbe uint32
	if err := binary.Read(file, binary.LittleEndian, &nClusters); err != nil {
		return fmt.Errorf("failed to read nClusters: %w", err)
	}
	if err := binary.Read(file, binary.LittleEndian, &nProbe); err != nil {
		return fmt.Errorf("failed to read nProbe: %w", err)
	}

	// Set configuration parameters from IVF file
	i.nClusters = int(nClusters)
	i.nProbe = int(nProbe)

	// Update config map for consistency
	if i.config == nil {
		i.config = make(map[string]any)
	}
	i.config["NClusters"] = int(nClusters)
	i.config["NProbe"] = int(nProbe)

	// Runtime state
	var centroidCount, size uint32
	if err := binary.Read(file, binary.LittleEndian, &centroidCount); err != nil {
		return fmt.Errorf("failed to read centroid count: %w", err)
	}
	if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
		return fmt.Errorf("failed to read size: %w", err)
	}

	i.size = int(size)
	i.centroids = make([]Centroid, 0, centroidCount)

	// Read centroids
	for j := uint32(0); j < centroidCount; j++ {
		var clusterID int32
		var vectorID uint64
		if err := binary.Read(file, binary.LittleEndian, &clusterID); err != nil {
			return fmt.Errorf("failed to read centroid ID: %w", err)
		}
		if err := binary.Read(file, binary.LittleEndian, &vectorID); err != nil {
			return fmt.Errorf("failed to read centroid vector ID: %w", err)
		}
		i.centroids = append(i.centroids, Centroid{
			ID:       int(clusterID),
			VectorID: vectorID,
		})
	}

	// Read cluster assignments
	var assignmentCount uint32
	if err := binary.Read(file, binary.LittleEndian, &assignmentCount); err != nil {
		return fmt.Errorf("failed to read assignment count: %w", err)
	}

	i.vectorToCluster = make(map[uint64]int, assignmentCount)
	i.clusters = make(map[int][]uint64)

	// Read each assignment and rebuild clusters map
	for j := uint32(0); j < assignmentCount; j++ {
		var vecID uint64
		var clusterID int32
		if err := binary.Read(file, binary.LittleEndian, &vecID); err != nil {
			if err == io.EOF {
				return fmt.Errorf("unexpected EOF while reading assignment %d", j)
			}
			return fmt.Errorf("failed to read vector ID: %w", err)
		}
		if err := binary.Read(file, binary.LittleEndian, &clusterID); err != nil {
			return fmt.Errorf("failed to read cluster ID: %w", err)
		}

		clusterIDInt := int(clusterID)
		i.vectorToCluster[vecID] = clusterIDInt
		// Rebuild clusters map
		i.clusters[clusterIDInt] = append(i.clusters[clusterIDInt], vecID)
	}

	return nil
}
