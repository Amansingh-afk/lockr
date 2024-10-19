package main

import (
	"fmt"
	"os"

	"Lockr/bin/cli"
)

// main is the entry point of the Lockr application
func main() {
	// Run the CLI and handle any errors
	if err := cli.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
