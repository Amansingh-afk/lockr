package cli

import (
	// "bufio"
	"fmt"
	"os"
	// "strings"

	"Lockr/bin/lsmtree"
)

// Run starts the CLI interface for the Lockr application
func Run() error {
	// Get the user's home directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to get user home directory: %w", err)
	}

	// Create the data directory in the user's home folder
	dataDir := fmt.Sprintf("%s/.Lockr", homeDir)
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Initialize the LSM tree
	lsm := lsmtree.NewLSMTree(dataDir)
	if err := lsm.Recover(); err != nil {
		return fmt.Errorf("failed to recover LSM tree: %w", err)
	}

	// Run the UI
	return RunUI(lsm)
}
