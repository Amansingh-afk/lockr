package lsmtree

// MemTable represents an in-memory key-value store
type MemTable struct {
	data map[string]string
}

// NewMemTable creates a new MemTable
func NewMemTable() *MemTable {
	return &MemTable{
		data: make(map[string]string),
	}
}

// Set adds or updates a key-value pair in the MemTable
func (m *MemTable) Set(key, value string) {
	m.data[key] = value
}

// Get retrieves the value for a given key from the MemTable
func (m *MemTable) Get(key string) (string, bool) {
	value, ok := m.data[key]
	return value, ok
}

// Delete removes a key-value pair from the MemTable
func (m *MemTable) Delete(key string) {
	delete(m.data, key)
}

// Size returns the number of entries in the MemTable
func (m *MemTable) Size() int {
	return len(m.data)
}

// Entries returns all key-value pairs in the MemTable
func (m *MemTable) Entries() map[string]string {
	return m.data
}
