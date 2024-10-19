package lsmtree

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// WAL represents a Write-Ahead Log
type WAL struct {
	filePath string
}

// NewWAL creates a new WAL with the given data directory
func NewWAL(dataDir string) *WAL {
	return &WAL{
		filePath: filepath.Join(dataDir, "wal.log"),
	}
}

// Log appends a key-value pair to the WAL
func (w *WAL) Log(key, value string) error {
	file, err := os.OpenFile(w.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer file.Close()

	entry := fmt.Sprintf("%s,%s\n", key, value)
	if _, err := file.WriteString(entry); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	return nil
}

// Recover reads the WAL and returns all key-value pairs
func (w *WAL) Recover() (map[string]string, error) {
	entries := make(map[string]string)

	file, err := os.Open(w.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return entries, nil
		}
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), ",", 2)
		if len(parts) == 2 {
			key, value := parts[0], parts[1]
			entries[key] = value
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read WAL: %w", err)
	}

	return entries, nil
}

// Clear truncates the WAL file, effectively clearing its contents
func (w *WAL) Clear() error {
	// Check if the file exists before attempting to truncate it
	if _, err := os.Stat(w.filePath); os.IsNotExist(err) {
		// File doesn't exist, so there's nothing to clear
		return nil
	}
	return os.Truncate(w.filePath, 0)
}
