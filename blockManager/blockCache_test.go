package blockManager

import (
	"testing"
)

func TestBlockCache_AddBlock(t *testing.T) {
	cache := InitBlockCache[*Block](2)
	block1 := &Block{filePath: "file1.txt", offset: 0, data: []byte("block1")}
	block2 := &Block{filePath: "file2.txt", offset: 1, data: []byte("block2")}

	cache.addBlock(block1)
	cache.addBlock(block2)

	if cache.size != 2 {
		t.Errorf("Expected cache size 2, got %d", cache.size)
	}
	if _, exists := cache.nodeMap[initKey("file1.txt", 0)]; !exists {
		t.Errorf("Expected block1 to be in cache")
	}
	if _, exists := cache.nodeMap[initKey("file2.txt", 1)]; !exists {
		t.Errorf("Expected block2 to be in cache")
	}
}

func TestBlockCache_FindBlock(t *testing.T) {
	cache := InitBlockCache[*Block](2)
	block1 := &Block{filePath: "file1.txt", offset: 0, data: []byte("block1")}
	cache.addBlock(block1)

	retrieved, found := cache.findBlock("file1.txt", 0)
	if !found {
		t.Errorf("Expected to find block1 in cache")
	}
	if retrieved != block1 {
		t.Errorf("Expected retrieved block to be block1")
	}
}

func TestBlockCache_Eviction(t *testing.T) {
	cache := InitBlockCache[*Block](2)
	block1 := &Block{filePath: "file1.txt", offset: 0, data: []byte("block1")}
	block2 := &Block{filePath: "file2.txt", offset: 1, data: []byte("block2")}
	block3 := &Block{filePath: "file3.txt", offset: 2, data: []byte("block3")}

	cache.addBlock(block1)
	cache.addBlock(block2)
	cache.addBlock(block3)

	if cache.size != 2 {
		t.Errorf("Expected cache size 2, got %d", cache.size)
	}
	if _, exists := cache.nodeMap[initKey("file1.txt", 0)]; exists {
		t.Errorf("Expected block1 to be evicted")
	}
	if _, exists := cache.nodeMap[initKey("file2.txt", 1)]; !exists {
		t.Errorf("Expected block2 to be in cache")
	}
	if _, exists := cache.nodeMap[initKey("file3.txt", 2)]; !exists {
		t.Errorf("Expected block3 to be in cache")
	}
}
