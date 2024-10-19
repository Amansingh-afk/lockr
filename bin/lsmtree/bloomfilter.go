package lsmtree

import (
	"hash/fnv"
)

// BloomFilter represents a probabilistic data structure for set membership testing
type BloomFilter struct {
	bitArray  []bool
	size      uint
	hashFuncs uint
}

// NewBloomFilter creates a new BloomFilter with default size and number of hash functions
func NewBloomFilter() *BloomFilter {
	size := uint(2097152) // 2MB
	hashFuncs := uint(7)
	return &BloomFilter{
		bitArray:  make([]bool, size),
		size:      size,
		hashFuncs: hashFuncs,
	}
}

// Add adds a key to the BloomFilter
func (bf *BloomFilter) Add(key string) {
	for i := uint(0); i < bf.hashFuncs; i++ {
		index := bf.hash(key, i)
		bf.bitArray[index] = true
	}
}

// MightContain checks if a key might be in the BloomFilter
func (bf *BloomFilter) MightContain(key string) bool {
	for i := uint(0); i < bf.hashFuncs; i++ {
		index := bf.hash(key, i)
		if !bf.bitArray[index] {
			return false
		}
	}
	return true
}

// hash generates a hash for a given key and seed
func (bf *BloomFilter) hash(key string, seed uint) uint {
	h := fnv.New64a()
	h.Write([]byte(key))
	h.Write([]byte{byte(seed)})
	return uint(h.Sum64() % uint64(bf.size))
}
