package lsmtree

import (
	"fmt"
)

// memTableSizeThreshold is the size limit for the MemTable before it's flushed to disk
const memTableSizeThreshold = 1024 * 1024 // 1MB

// LSMTree represents a Log-Structured Merge Tree
type LSMTree struct {
	dataDir  string
	memTable *MemTable
	ssTables []*SSTable
	wal      *WAL
}

// NewLSMTree creates a new LSMTree with the given data directory
func NewLSMTree(dataDir string) *LSMTree {
	return &LSMTree{
		dataDir:  dataDir,
		memTable: NewMemTable(),
		ssTables: make([]*SSTable, 0),
		wal:      NewWAL(dataDir),
	}
}

// Set adds or updates a key-value pair in the LSMTree
func (l *LSMTree) Set(key, value string) error {
	// Log the operation to the WAL
	if err := l.wal.Log(key, value); err != nil {
		return fmt.Errorf("failed to log to WAL: %w", err)
	}

	// Add the key-value pair to the MemTable
	l.memTable.Set(key, value)

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
	// First, check the MemTable
	if value, ok := l.memTable.Get(key); ok {
		return value, nil
	}

	// If not found in MemTable, search through SSTables from newest to oldest
	for i := len(l.ssTables) - 1; i >= 0; i-- {
		value, err := l.ssTables[i].Get(key)
		if err != nil {
			return "", fmt.Errorf("failed to get value from SSTable: %w", err)
		}
		if value != "" {
			return value, nil
		}
	}

	// Key not found
	return "", nil
}

// Delete removes a key-value pair from the LSMTree
func (l *LSMTree) Delete(key string) error {
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

	return nil
}

// List returns all non-deleted key-value pairs in the LSMTree
func (l *LSMTree) List() (map[string]string, error) {
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
