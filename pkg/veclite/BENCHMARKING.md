# Benchmarking with Real Features

This guide explains how to use non-random features for more accurate benchmarking.

## Why Use Real Features?

Random vectors are uniformly distributed in space, which creates a poor graph structure for HNSW. Real-world embeddings have:
- **Clusters**: Similar items are grouped together
- **Structure**: Meaningful relationships between vectors
- **Locality**: Nearby vectors in embedding space are semantically similar

HNSW performs significantly better on structured data, so using real features gives more accurate performance measurements.

## Option 1: Clustered Synthetic Data

The benchmark includes `generateClusteredVector()` which creates vectors in clusters:

```go
// Generate a vector from cluster 5 out of 50 clusters
vector := generateClusteredVector(128, 5, 50, seed)
```

This creates Gaussian clusters that better simulate real-world data distribution.

**Usage:**
```bash
go test ./pkg/veclite -bench=BenchmarkSearch_HNSW_Clustered -run='^$'
```

## Option 2: Real Embeddings

### Generating Embeddings

#### Using Python (sentence-transformers)

```python
from sentence_transformers import SentenceTransformer
import numpy as np

# Load model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Generate embeddings
texts = ["Your text data here..."] * 10000
embeddings = model.encode(texts)

# Save as binary file
embeddings.astype('float32').tofile('embeddings.bin')

# Save metadata (num_vectors, dimension)
with open('embeddings.meta', 'w') as f:
    f.write(f"{len(embeddings)}\n{embeddings.shape[1]}\n")
```

#### Using Python (BERT)

```python
from transformers import AutoTokenizer, AutoModel
import torch
import numpy as np

tokenizer = AutoTokenizer.from_pretrained('bert-base-uncased')
model = AutoModel.from_pretrained('bert-base-uncased')

def get_embedding(text):
    inputs = tokenizer(text, return_tensors='pt', padding=True, truncation=True)
    with torch.no_grad():
        outputs = model(**inputs)
    # Use [CLS] token embedding
    return outputs.last_hidden_state[:, 0, :].numpy()

# Generate and save embeddings
embeddings = np.array([get_embedding(text) for text in texts])
embeddings.astype('float32').tofile('embeddings.bin')
```

### Loading Embeddings in Go

Add this to your benchmark file:

```go
import (
    "encoding/binary"
    "os"
)

func loadEmbeddingsFromBinary(filename string) ([][]float32, error) {
    file, err := os.Open(filename)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    // Read metadata
    var numVectors, dimension uint32
    binary.Read(file, binary.LittleEndian, &numVectors)
    binary.Read(file, binary.LittleEndian, &dimension)

    vectors := make([][]float32, numVectors)
    for i := uint32(0); i < numVectors; i++ {
        vector := make([]float32, dimension)
        binary.Read(file, binary.LittleEndian, &vector)
        vectors[i] = vector
    }

    return vectors, nil
}

// Use in benchmark
func BenchmarkSearch_HNSW_RealEmbeddings(b *testing.B) {
    vectors, err := loadEmbeddingsFromBinary("embeddings.bin")
    if err != nil {
        b.Fatalf("Failed to load embeddings: %v", err)
    }

    db, cleanup := createBenchmarkDB(b, "hnsw")
    defer cleanup()

    // Insert real embeddings
    for i, vec := range vectors {
        db.Insert(uint64(i+1), vec)
    }

    // Use random vectors from the dataset as queries
    // ...
}
```

## Option 3: CSV Format

If your embeddings are in CSV format:

```go
import (
    "encoding/csv"
    "strconv"
    "os"
)

func loadEmbeddingsFromCSV(filename string) ([][]float32, error) {
    file, err := os.Open(filename)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    reader := csv.NewReader(file)
    records, err := reader.ReadAll()
    if err != nil {
        return nil, err
    }

    vectors := make([][]float32, len(records))
    for i, record := range records {
        vector := make([]float32, len(record))
        for j, val := range record {
            f, _ := strconv.ParseFloat(val, 32)
            vector[j] = float32(f)
        }
        vectors[i] = vector
    }

    return vectors, nil
}
```

## Recommended Datasets

For testing, you can use:

1. **SIFT/SIFT1M**: Image feature vectors
   - Download from: http://corpus-texmex.irisa.fr/
   - 128-dimensional vectors

2. **GloVe Embeddings**: Word embeddings
   - Download from: https://nlp.stanford.edu/projects/glove/
   - Various dimensions (50, 100, 200, 300)

3. **MNIST Embeddings**: Generate from MNIST images
   - Use a CNN to generate embeddings from MNIST images

4. **Custom Embeddings**: Generate from your own data
   - Text: sentence-transformers, BERT
   - Images: ResNet, VGG features
   - Audio: Wav2Vec, Whisper

## Example: Complete Benchmark with Real Data

```go
func BenchmarkSearch_HNSW_RealData(b *testing.B) {
    // Load pre-computed embeddings
    vectors, err := loadEmbeddingsFromBinary("my_embeddings.bin")
    if err != nil {
        b.Skipf("Skipping: embeddings file not found: %v", err)
    }

    const k = 10
    db, cleanup := createBenchmarkDB(b, "hnsw")
    defer cleanup()

    // Insert all vectors
    for i, vec := range vectors {
        if err := db.Insert(uint64(i+1), vec); err != nil {
            b.Fatalf("Failed to insert: %v", err)
        }
    }

    // Use random vectors from dataset as queries
    queries := make([][]float32, b.N)
    rng := rand.New(rand.NewSource(42))
    for i := 0; i < b.N; i++ {
        queryIdx := rng.Intn(len(vectors))
        queries[i] = vectors[queryIdx]
    }

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, err := db.Search(queries[i], k)
        if err != nil {
            b.Fatalf("Search failed: %v", err)
        }
    }
}
```

## Tips

1. **Normalize vectors**: Some embedding models produce unnormalized vectors. Consider normalizing:
   ```go
   func normalize(v []float32) {
       var sum float32
       for _, x := range v {
           sum += x * x
       }
       norm := float32(math.Sqrt(float64(sum)))
       for i := range v {
           v[i] /= norm
       }
   }
   ```

2. **Use appropriate distance**: L2 distance works well for most embeddings, but cosine distance might be better for normalized vectors.

3. **Warm up cache**: Run a few searches before benchmarking to warm up the cache.

4. **Compare both**: Run benchmarks with both random and real data to see the difference in HNSW performance.

