// Test used for earlier version of sstable
package ssTable

// import (
// 	"NASP2024/blockManager"
// 	"bytes"
// 	"encoding/binary"
// 	"hash/crc32"
// 	"os"
// 	"testing"
// )

// // TestSingleEntryFitsInOneBlock tests a single entry that fits entirely within one block.
// func TestSingleEntryFitsInOneBlock(t *testing.T) {
// 	filePath := "test_single_entry.dat"
// 	defer os.Remove(filePath)

// 	key := []byte("key")
// 	value := []byte("value")
// 	data := createEntry(key, value)

// 	var block *blockManager.Block
// 	for block = range PrepareBlocks(filePath, data) {
// 		expectedCRC := crc32.ChecksumIEEE(block.GetData()[4:])
// 		if binary.BigEndian.Uint32(block.GetData()[0:4]) != expectedCRC {
// 			t.Errorf("CRC mismatch")
// 		}

// 		blockData := block.GetData()[9 : 9+len(data)]
// 		if !bytes.Equal(blockData, data) {
// 			t.Errorf("Data mismatch")
// 		}
// 	}
// }

// // TestMultipleEntriesFitInOneBlock tests multiple entries fitting in one block.
// func TestMultipleEntriesFitInOneBlock(t *testing.T) {
// 	filePath := "test_multiple_entries.dat"
// 	defer os.Remove(filePath)

// 	entries := [][]byte{
// 		createEntry([]byte("key1"), []byte("value1")),
// 		createEntry([]byte("key2"), []byte("value2")),
// 	}
// 	data := bytes.Join(entries, nil)

// 	ch := PrepareBlocks(filePath, data)
// 	block := <-ch

// 	expectedData := block.GetData()[9 : 9+len(data)]
// 	if !bytes.Equal(expectedData, data) {
// 		t.Errorf("Data mismatch")
// 	}
// }

// // TestEntrySpansMultipleBlocks tests a large entry spanning multiple blocks.
// func TestEntrySpansMultipleBlocks(t *testing.T) {
// 	filePath := "test_large_entry.dat"
// 	defer os.Remove(filePath)

// 	key := []byte("key")
// 	value := make([]byte, blockSize*2)
// 	data := createEntry(key, value)

// 	ch := PrepareBlocks(filePath, data)

// 	var reconstructed []byte
// 	for block := range ch {
// 		start := 9
// 		end := start + len(data) - len(reconstructed)
// 		if end > blockSize {
// 			end = blockSize
// 		}
// 		reconstructed = append(reconstructed, block.GetData()[9:end]...)
// 	}

// 	if !bytes.Equal(reconstructed, data) {
// 		t.Errorf("Data mismatch")
// 	}
// }

// // TestBoundaryConditions tests an entry exactly filling a block.
// func TestBoundaryConditions(t *testing.T) {
// 	filePath := "test_boundary.dat"
// 	defer os.Remove(filePath)

// 	key := []byte("k")
// 	valueSize := blockSize - 9 - 37 - len(key) // Adjust to fill exactly
// 	value := make([]byte, valueSize)
// 	data := createEntry(key, value)

// 	ch := PrepareBlocks(filePath, data)
// 	block := <-ch

// 	if !bytes.Equal(block.GetData()[9:9+len(data)], data) {
// 		t.Errorf("Data mismatch")
// 	}
// }

// func createEntry(key, value []byte) []byte {
// 	entry := make([]byte, 37+len(key)+len(value))
// 	binary.BigEndian.PutUint64(entry[21:29], uint64(len(key)))
// 	binary.BigEndian.PutUint64(entry[29:37], uint64(len(value)))
// 	copy(entry[37:37+len(key)], key)
// 	copy(entry[37+len(key):], value)
// 	return entry
// }
