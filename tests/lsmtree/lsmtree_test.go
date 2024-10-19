package lsmtree_test

import (
	"testing"
	"Lockr/bin/lsmtree"
)

// TestLSMTreeSetGet tests the Set and Get operations of the LSMTree
func TestLSMTreeSetGet(t *testing.T) {
	// Create a new LSMTree with a temporary directory
	tree := lsmtree.NewLSMTree("/tmp/lsm-test")

	// Set a test key-value pair
	err := tree.Set("foo", "bar")
	if err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	// Retrieve the value for the test key
	value, err := tree.Get("testKey")
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	// Check if the retrieved value matches the expected value
	if value != "testValue" {
		t.Errorf("Expected 'testValue', got '%s'", value)
	}
}
