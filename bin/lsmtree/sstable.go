package lsmtree

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// SSTable represents a Sorted String Table, an immutable on-disk data structure
type SSTable struct {
	filePath    string
	bloomFilter *BloomFilter
	index       map[string]int64
}

// NewSSTable creates a new SSTable from the given MemTable
func NewSSTable(dataDir string, memTable *MemTable) (*SSTable, error) {
	// Generate a unique filename based on the current timestamp
	timestamp := time.Now().UnixNano()
	filePath := filepath.Join(dataDir, fmt.Sprintf("sstable_%d.dat", timestamp))

	// Create the SSTable file
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable file: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	bloomFilter := NewBloomFilter()
	index := make(map[string]int64)

	// Write entries to the SSTable file and update the index and bloom filter
	var offset int64
	for key, value := range memTable.Entries() {
		entry := fmt.Sprintf("%s,%s\n", key, value)
		_, err := writer.WriteString(entry)
		if err != nil {
			return nil, fmt.Errorf("failed to write entry to SSTable: %w", err)
		}

		bloomFilter.Add(key)
		index[key] = offset
		offset += int64(len(entry))
	}

	if err := writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush SSTable: %w", err)
	}

	return &SSTable{
		filePath:    filePath,
		bloomFilter: bloomFilter,
		index:       index,
	}, nil
}

// Get retrieves the value for a given key from the SSTable
func (s *SSTable) Get(key string) (string, error) {
	// Check if the key might be in the SSTable using the bloom filter
	if !s.bloomFilter.MightContain(key) {
		return "", nil
	}

	// Check if the key is in the index
	offset, ok := s.index[key]
	if !ok {
		return "", nil
	}

	// Open the SSTable file
	file, err := os.Open(s.filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open SSTable file: %w", err)
	}
	defer file.Close()

	// Seek to the correct position in the file
	_, err = file.Seek(offset, 0)
	if err != nil {
		return "", fmt.Errorf("failed to seek in SSTable file: %w", err)
	}

	// Read the entry and return the value if found
	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), ",", 2)
		if len(parts) == 2 && parts[0] == key {
			return parts[1], nil
		}
	}

	return "", nil
}

// FilePath returns the file path of the SSTable
func (s *SSTable) FilePath() string {
	return s.filePath
}

// Add this method to the SSTable struct

// List returns all non-deleted key-value pairs in the SSTable
func (s *SSTable) List() (map[string]string, error) {
	result := make(map[string]string)

	file, err := os.Open(s.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SSTable file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), ",", 2)
		if len(parts) == 2 {
			key, value := parts[0], parts[1]
			if value != "" {
				result[key] = value
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read SSTable: %w", err)
	}

	return result, nil
}
