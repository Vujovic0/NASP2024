package systemOps

import (
	"github.com/Vujovic0/NASP2024/lruCache"
	"github.com/Vujovic0/NASP2024/memtableStructures"
	"github.com/Vujovic0/NASP2024/ssTable"
	"github.com/Vujovic0/NASP2024/wal"
)

// Upis stanja token bucket-a (sistemski poziv)
func SystemPut(walFactory *wal.WAL, mtm *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache, key string, value string) {
	// if walFactory == nil {
	// 	return
	// }
	offset, err := (*walFactory).WriteLogEntry(key, []byte(value), false)
	if err == nil {
		mtm.Insert(key, []byte(value), false, walFactory.CurrentFile.Name(), walFactory.CurrentBlock, offset)
		lruCache.Put(key, []byte(value))
	}
}

// Citanje stanja token bucket-a (sistemski poziv)
func SystemGet(lruCache *lruCache.LRUCache, memtableMenager *memtableStructures.MemTableManager, key string) (string, bool) {
	element, found := memtableMenager.Search(key)
	if found {
		return string(element.Value), true
	}
	value, found := lruCache.Get(key)
	if found {
		return string(value), true
	}
	valueBytes := ssTable.SearchAll([]byte(key), false)
	if len(valueBytes) > 0 {
		return string(valueBytes), true
	}
	return "", false
}
