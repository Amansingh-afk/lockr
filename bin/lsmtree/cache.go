package lsmtree

import (
	"sync"
	"time"
)

type CacheEntry struct {
	value     string
	timestamp time.Time
}

type Cache struct {
	entries     map[string]CacheEntry
	mutex       sync.RWMutex
	maxSize     int
	accessCount map[string]int
}

func NewCache(maxSize int) *Cache {
	return &Cache{
		entries:     make(map[string]CacheEntry),
		maxSize:     maxSize,
		accessCount: make(map[string]int),
	}
}

func (c *Cache) Set(key, value string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.entries) >= c.maxSize {
		c.evict()
	}

	c.entries[key] = CacheEntry{value: value, timestamp: time.Now()}
	c.accessCount[key] = 1
}

func (c *Cache) Get(key string) (string, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if entry, ok := c.entries[key]; ok {
		c.accessCount[key]++
		return entry.value, true
	}
	return "", false
}

func (c *Cache) evict() {
	var leastAccessed string
	minCount := int(^uint(0) >> 1) // Max int value

	for key, count := range c.accessCount {
		if count < minCount {
			minCount = count
			leastAccessed = key
		}
	}

	delete(c.entries, leastAccessed)
	delete(c.accessCount, leastAccessed)
}

