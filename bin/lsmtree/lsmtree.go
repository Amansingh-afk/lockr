package lsmtree

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// memTableSizeThreshold is the size limit for the MemTable before it's flushed to disk
const memTableSizeThreshold = 1024 * 1024 // 1MB

// LSMTree represents a Log-Structured Merge Tree
type LSMTree struct {
	dataDir  string
	memTable *MemTable
	ssTables []*SSTable
	wal      *WAL
	mutex    sync.RWMutex
	cache    *Cache
}

// NewLSMTree creates a new LSMTree with the given data directory
func NewLSMTree(dataDir string) *LSMTree {
	return &LSMTree{
		dataDir:  dataDir,
		memTable: NewMemTable(),
		ssTables: make([]*SSTable, 0),
		wal:      NewWAL(dataDir),
		cache:    NewCache(1000), // Cache with 1000 entries
	}
}

// Set adds or updates a key-value pair in the LSMTree
func (l *LSMTree) Set(key, value string) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// Log the operation to the WAL
	if err := l.wal.Log(key, value); err != nil {
		return fmt.Errorf("failed to log to WAL: %w", err)
	}

	// Add the key-value pair to the MemTable
	l.memTable.Set(key, value)

	// Update the cache
	l.cache.Set(key, value)

	// If the MemTable size exceeds the threshold, flush it to disk
	if l.memTable.Size() >= memTableSizeThreshold {
		if err := l.flushMemTable(); err != nil {
			return fmt.Errorf("failed to flush memtable: %w", err)
		}
	}

	return nil
}

// Get retrieves the value for a given key from the LSMTree
func (l *LSMTree) Get(key string) (string, error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	// First, check the cache
	if value, ok := l.cache.Get(key); ok {
		return value, nil
	}

	// Then, check the MemTable
	if value, ok := l.memTable.Get(key); ok {
		l.cache.Set(key, value)
		return value, nil
	}

	// If not found in MemTable, search through SSTables from newest to oldest
	for i := len(l.ssTables) - 1; i >= 0; i-- {
		value, err := l.ssTables[i].Get(key)
		if err != nil {
			return "", fmt.Errorf("failed to get value from SSTable: %w", err)
		}
		if value != "" {
			l.cache.Set(key, value)
			return value, nil
		}
	}

	// Key not found
	return "", nil
}

// Delete removes a key-value pair from the LSMTree
func (l *LSMTree) Delete(key string) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// First, check if the key exists
	value, err := l.Get(key)
	if err != nil {
		return fmt.Errorf("failed to check key existence: %w", err)
	}
	if value == "" {
		return fmt.Errorf("key not found")
	}

	// If the key exists, mark it as deleted by setting an empty value
	err = l.Set(key, "")
	if err != nil {
		return fmt.Errorf("failed to mark key as deleted: %w", err)
	}

	return nil
}

// Recover rebuilds the MemTable from the WAL
func (l *LSMTree) Recover() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	entries, err := l.wal.Recover()
	if err != nil {
		return fmt.Errorf("failed to recover from WAL: %w", err)
	}

	// Replay the entries from the WAL into the MemTable
	for key, value := range entries {
		l.memTable.Set(key, value)
	}

	// Clear the WAL if it exists and we successfully recovered entries
	if len(entries) > 0 {
		if err := l.wal.Clear(); err != nil {
			return fmt.Errorf("failed to clear WAL: %w", err)
		}
	}

	return nil
}

// flushMemTable writes the current MemTable to disk as an SSTable
func (l *LSMTree) flushMemTable() error {
	ssTable, err := NewSSTable(l.dataDir, l.memTable)
	if err != nil {
		return fmt.Errorf("failed to create SSTable: %w", err)
	}

	l.ssTables = append(l.ssTables, ssTable)
	l.memTable = NewMemTable()

	// Trigger compaction after flushing
	go l.triggerCompaction()

	return nil
}

// List returns all non-deleted key-value pairs in the LSMTree
func (l *LSMTree) List() (map[string]string, error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	result := make(map[string]string)

	// First, add all entries from the MemTable
	for key, value := range l.memTable.Entries() {
		if value != "" {
			result[key] = value
		}
	}

	// Then, iterate through SSTables from newest to oldest
	for i := len(l.ssTables) - 1; i >= 0; i-- {
		entries, err := l.ssTables[i].List()
		if err != nil {
			return nil, fmt.Errorf("failed to list entries from SSTable: %w", err)
		}
		for key, value := range entries {
			if _, exists := result[key]; !exists {
				if value != "" {
					result[key] = value
				}
			}
		}
	}

	return result, nil
}

// triggerCompaction initiates the compaction process
func (l *LSMTree) triggerCompaction() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if len(l.ssTables) < 2 {
		return // Not enough SSTables to compact
	}

	// Compact the two oldest SSTables
	oldestSSTable := l.ssTables[0]
	secondOldestSSTable := l.ssTables[1]

	compactedSSTable, err := l.compactSSTables(oldestSSTable, secondOldestSSTable)
	if err != nil {
		fmt.Printf("Error during compaction: %v\n", err)
		return
	}

	// Remove the two old SSTables and add the new compacted one
	l.ssTables = append([]*SSTable{compactedSSTable}, l.ssTables[2:]...)

	// Clean up old SSTable files
	if err := os.Remove(oldestSSTable.FilePath()); err != nil {
		fmt.Printf("Error removing old SSTable file: %v\n", err)
	}
	if err := os.Remove(secondOldestSSTable.FilePath()); err != nil {
		fmt.Printf("Error removing old SSTable file: %v\n", err)
	}
}

// compactSSTables merges two SSTables into a new one
func (l *LSMTree) compactSSTables(ssTable1, ssTable2 *SSTable) (*SSTable, error) {
	mergedEntries := make(map[string]string)

	// Merge entries from both SSTables
	for _, ssTable := range []*SSTable{ssTable1, ssTable2} {
		entries, err := ssTable.List()
		if err != nil {
			return nil, fmt.Errorf("failed to list entries from SSTable: %w", err)
		}
		for key, value := range entries {
			mergedEntries[key] = value
		}
	}

	// Create a new MemTable with the merged entries
	mergedMemTable := NewMemTable()
	for key, value := range mergedEntries {
		mergedMemTable.Set(key, value)
	}

	// Create a new SSTable from the merged MemTable
	timestamp := time.Now().UnixNano()
	compactedSSTablePath := filepath.Join(l.dataDir, fmt.Sprintf("sstable_compacted_%d.dat", timestamp))
	compactedSSTable, err := NewSSTable(compactedSSTablePath, mergedMemTable)
	if err != nil {
		return nil, fmt.Errorf("failed to create compacted SSTable: %w", err)
	}

	return compactedSSTable, nil
}
