package cli

import (
	"bufio"
	"fmt"
	"os"
	"strings"

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

	// Create a new reader for user input
	reader := bufio.NewReader(os.Stdin)

	// Main loop for CLI interaction
	for {
		fmt.Print("Lockr> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		// Parse the input command
		parts := strings.Fields(input)
		if len(parts) == 0 {
			fmt.Println("Please enter a command.")
			continue
		}

		command := parts[0]

		// Handle commands based on their expected number of arguments
		switch command {
		case "list", "clear", "quit", "exit":
			if len(parts) != 1 {
				fmt.Printf("Invalid '%s' command. Use '%s' without any arguments.\n", command, command)
				continue
			}
			switch command {
			case "list":
				entries, err := lsm.List()
				if err != nil {
					fmt.Printf("Error listing entries: %v\n", err)
				} else {
					if len(entries) == 0 {
						fmt.Println("No entries found")
					} else {
						for k, v := range entries {
							fmt.Printf("%s: %s\n", k, v)
						}
					}
				}
			case "clear":
				// Clear the screen (implementation depends on the OS)
				fmt.Print("\033[H\033[2J")
			case "quit", "exit":
				fmt.Println("Goodbye!")
				return nil
			}
		case "set":
			if len(parts) != 3 {
				fmt.Println("Invalid 'set' command. Use 'set <key> <value>'.")
				continue
			}
			key := parts[1]
			value := parts[2]
			if err := lsm.Set(key, value); err != nil {
				fmt.Printf("Error setting value: %v\n", err)
			} else {
				fmt.Printf("Set %s to %s\n", key, value)
			}
		case "get", "delete":
			if len(parts) != 2 {
				fmt.Printf("Invalid '%s' command. Use '%s <key>'.\n", command, command)
				continue
			}
			key := parts[1]
			switch command {
			case "get":
				value, err := lsm.Get(key)
				if err != nil {
					fmt.Printf("Error getting value: %v\n", err)
				} else if value == "" {
					fmt.Printf("Key %s not found\n", key)
				} else {
					fmt.Printf("%s\n", value)
				}
			case "delete":
				err := lsm.Delete(key)
				if err != nil {
					if err.Error() == "key not found" {
						fmt.Printf("Key %s not found\n", key)
					} else {
						fmt.Printf("Error deleting key: %v\n", err)
					}
				} else {
					fmt.Printf("Deleted %s\n", key)
				}
			}
		default:
			fmt.Println("Invalid command. Use 'set <key> <value>', 'get <key>', 'delete <key>', 'list', 'clear', or 'quit'.")
			continue
		}
	}

	return nil
}
