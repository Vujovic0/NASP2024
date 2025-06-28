package ssTable

import (
	"NASP2024/config"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"strings"
	"testing"
)

// Helper functions
func checkEqual(t *testing.T, got, want interface{}, msg string) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %v, want %v", msg, got, want)
	}
}

func checkNil(t *testing.T, got interface{}, msg string) {
	t.Helper()
	if got != nil {
		t.Errorf("%s: expected nil, got %v", msg, got)
	}
}

func checkNotNil(t *testing.T, got interface{}, msg string) {
	t.Helper()
	if got == nil {
		t.Errorf("%s: unexpected nil value", msg)
	}
}

func TestSingleEntryFitsInOneBlock(t *testing.T) {
	filePath := "test_single_entry.dat"
	defer os.Remove(filePath)

	key := "key"
	value := "value"
	data := SerializeEntryHelper(key, value, false, false)

	var block *channelResult
	for block = range PrepareSSTableBlocks(filePath, data, true, 0, false) {
		if block.Block == nil {
			t.Errorf("Expected block, got nil")
		}
		expectedCRC := crc32.ChecksumIEEE(block.Block.GetData()[4:])
		if binary.LittleEndian.Uint32(block.Block.GetData()[0:4]) != expectedCRC {
			t.Errorf("CRC mismatch")
		}
		blockData := block.Block.GetData()[9:]
		if !bytes.Equal(blockData[:block.Block.GetSize()], data) {
			t.Errorf("Data mismatch")
		}
	}
}

func TestMultipleEntriesFitInOneBlock(t *testing.T) {
	filePath := "test_multiple_entries.dat"
	defer os.Remove(filePath)

	entries := [][]byte{
		SerializeEntryHelper("key1", "value1", false, false),
		SerializeEntryHelper("key2", "value2", false, false),
	}
	data := bytes.Join(entries, nil)

	ch := PrepareSSTableBlocks(filePath, data, true, 0, false)
	block := <-ch

	expectedData := block.Block.GetData()[9:]
	if !bytes.Equal(expectedData[:block.Block.GetSize()], data) {
		t.Errorf("Data mismatch")
	}
}

func TestEntrySpansMultipleBlocks(t *testing.T) {
	filePath := "test_large_entry.dat"
	defer os.Remove(filePath)

	key := "key"
	value := string(make([]byte, blockSize*2))
	data := SerializeEntryHelper(key, value, false, false)

	ch := PrepareSSTableBlocks(filePath, data, true, 0, false)

	var reconstructed []byte
	for block := range ch {
		start := 9
		reconstructed = append(reconstructed, block.Block.GetData()[start:block.Block.GetSize()+9]...)
	}

	if !bytes.Equal(reconstructed, data) {
		t.Errorf("Data mismatch")
	}
}

func TestBoundaryConditions(t *testing.T) {
	filePath := "test_boundary.dat"
	defer os.Remove(filePath)

	key := "k"
	valueSize := int(blockSize) - 9 - 29 - len(key)
	value := string(make([]byte, valueSize))
	data := SerializeEntryHelper(key, value, false, false)

	ch := PrepareSSTableBlocks(filePath, data, true, 0, false)
	block := <-ch

	if !bytes.Equal(block.Block.GetData()[9:9+block.Block.GetSize()], data) {
		t.Errorf("Data mismatch")
	}
}

func TestBasicWriteAndRead(t *testing.T) {
	// Setup
	os.RemoveAll("data")

	key := "testKey"
	value := "testValue"
	data := SerializeEntryHelper(key, value, false, false)
	lastKeyData := SerializeEntryHelper(key, "", false, true)

	// Test separated SSTable
	t.Run("Separated SSTable", func(t *testing.T) {
		CreateSeparatedSSTable(data, lastKeyData, 1, 1)
		result := Find([]byte(key))
		checkNotNil(t, result, "Should find value in separated SSTable")
		checkEqual(t, string(result), value, "Incorrect value in separated SSTable")
	})

	// Test compact SSTable
	t.Run("Compact SSTable", func(t *testing.T) {
		CreateCompactSSTable(data, lastKeyData, 1, 1)
		result := Find([]byte(key))
		checkNotNil(t, result, "Should find value in compact SSTable")
		checkEqual(t, string(result), value, "Incorrect value in compact SSTable")
	})
}

func TestTombstoneHandlingSeparated(t *testing.T) {
	os.RemoveAll("data")

	key := "deletedKey"
	data := SerializeEntryHelper(key, "", true, false)
	lastKeyData := SerializeEntryHelper(key, "", true, true)

	CreateSeparatedSSTable(data, lastKeyData, 1, 1)
	result := Find([]byte(key))

	// Check that the result is an empty slice instead of nil
	if result == nil || len(result) != 0 {
		t.Errorf("Expected an empty slice for tombstoned key, got: %v", result)
	}
}

func TestTombstoneHandlingCompact(t *testing.T) {
	os.RemoveAll("data")

	key := "deletedKey"
	data := SerializeEntryHelper(key, "", true, false)
	lastKeyData := SerializeEntryHelper(key, "", true, true)

	CreateCompactSSTable(data, lastKeyData, 1, 1)
	result := Find([]byte(key))

	// Check that the result is an empty slice instead of nil
	if result == nil || len(result) != 0 {
		t.Errorf("Expected an empty slice for tombstoned key, got: %v", result)
	}
}

// TestMultiBlockEntries tests large values spanning multiple blocks
func TestFindMultiBlockEntriesCompact(t *testing.T) {
	os.RemoveAll("data")

	var bigData []byte
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := strings.Repeat("x", int(blockSize)*2) // 2-block values
		bigData = append(bigData, SerializeEntryHelper(key, value, false, false)...)
	}
	lastKeyData := SerializeEntryHelper(fmt.Sprintf("key%04d", 9), "", false, true)

	CreateCompactSSTable(bigData, lastKeyData, 15, 15)

	t.Run("RandomAccess", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			testKey := fmt.Sprintf("key%04d", i)
			result := Find([]byte(testKey))
			checkNotNil(t, result, "Missing value for "+testKey)
			checkEqual(t, len(result), int(blockSize)*2, "Incorrect value length")
		}
	})
}

func TestFindMultiBlockEntriesSeparated(t *testing.T) {
	os.RemoveAll("data")

	var bigData []byte
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := strings.Repeat("x", int(blockSize)*2) // 2-block values
		bigData = append(bigData, SerializeEntryHelper(key, value, false, false)...)
	}
	lastKeyData := SerializeEntryHelper(fmt.Sprintf("key%04d", 9), "", false, true)

	CreateSeparatedSSTable(bigData, lastKeyData, 1, 8)

	t.Run("RandomAccess", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			testKey := fmt.Sprintf("key%04d", i)
			result := Find([]byte(testKey))
			checkNotNil(t, result, "Missing value for "+testKey)
			checkEqual(t, len(result), int(blockSize)*2, "Incorrect value length")
		}
	})
}

func TestFindNonExistentKeys(t *testing.T) {
	os.RemoveAll("data")

	var data []byte
	keys := []string{"key0010", "key0020", "key0030"}
	for _, key := range keys {
		value := "val_" + key
		data = append(data, SerializeEntryHelper(key, value, false, false)...)
	}
	lastKeyData := SerializeEntryHelper("key0030", "", false, true)

	CreateCompactSSTable(data, lastKeyData, len(keys), len(keys))
	CreateSeparatedSSTable(data, lastKeyData, len(keys), len(keys))

	testCases := []struct {
		name     string
		key      string
		expected []byte
	}{
		{"NonExistentKey", "key9999", nil},
		{"KeyBeforeFirst", "key0001", nil},
		{"KeyAfterLast", "key0040", nil},
	}

	t.Run("Compact SSTable", func(t *testing.T) {
		for _, tc := range testCases {
			result := Find([]byte(tc.key))
			if result != nil && len(result) != 0 {
				t.Errorf("Compact - %s: expected nil or empty, got: %v", tc.name, result)
			}
		}
	})

	t.Run("Separated SSTable", func(t *testing.T) {
		for _, tc := range testCases {
			result := Find([]byte(tc.key))
			if result != nil && len(result) != 0 {
				t.Errorf("Separated - %s: expected nil or empty, got: %v", tc.name, result)
			}
		}
	})
}

func TestMultipleKeysInBlock0(t *testing.T) {
	os.RemoveAll("data")

	// Create several small entries that will all fit inside the first block
	var data []byte
	keys := []string{"a", "b", "c", "d", "e", "f", "g"}
	values := []string{"val_a", "val_b", "val_c", "val_d", "val_e", "val_f", "val_g"}

	for i := range keys {
		data = append(data, SerializeEntryHelper(keys[i], values[i], false, false)...)
	}
	lastKeyData := SerializeEntryHelper(keys[len(keys)-1], "", false, true)

	// Create both SSTable formats to test
	CreateCompactSSTable(data, lastKeyData, 2, 2)
	CreateSeparatedSSTable(data, lastKeyData, 2, 2)

	t.Run("Compact SSTable", func(t *testing.T) {
		for i, key := range keys {
			result := Find([]byte(key))
			checkNotNil(t, result, "Compact: missing result for "+key)
			checkEqual(t, string(result), values[i], "Compact: wrong value for "+key)
		}
	})

	t.Run("Separated SSTable", func(t *testing.T) {
		for i, key := range keys {
			result := Find([]byte(key))
			checkNotNil(t, result, "Separated: missing result for "+key)
			checkEqual(t, string(result), values[i], "Separated: wrong value for "+key)
		}
	})
}

func TestFindFullBlockEntriesCompact(t *testing.T) {
	os.RemoveAll("data")

	var bigData []byte
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := strings.Repeat("x", 1)
		bigData = append(bigData, SerializeEntryHelper(key, value, false, false)...)
	}
	lastKeyData := SerializeEntryHelper(fmt.Sprintf("key%04d", 9), "", false, true)

	CreateCompactSSTable(bigData, lastKeyData, 5, 5)

	t.Run("RandomAccess", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			testKey := fmt.Sprintf("key%04d", i)
			result := Find([]byte(testKey))
			checkNotNil(t, result, "Missing value for "+testKey)
			checkEqual(t, len(result), 1, "Incorrect value length")
		}
	})
}

func TestSearchThroughManyTables(t *testing.T) {
	os.RemoveAll("data")

	// Create several small entries that will all fit inside the first block
	var data1 []byte
	var data2 []byte
	keys1 := []string{"a", "b", "c", "d", "e", "f", "g"}
	keys2 := []string{"h", "i", "j", "k", "l", "m", "n"}
	values1 := []string{"val_a", "val_b", "val_c", "val_d", "val_e", "val_f", "val_g"}
	values2 := []string{"val_h", "val_i", "val_j", "val_k", "val_l", "val_m", "val_n"}

	for i := range keys1 {
		data1 = append(data1, SerializeEntryHelper(keys1[i], values1[i], false, false)...)
	}
	lastKeyData1 := SerializeEntryHelper(keys1[len(keys1)-1], "", false, true)

	for i := range keys2 {
		data2 = append(data2, SerializeEntryHelper(keys2[i], values2[i], false, false)...)
	}
	lastKeyData2 := SerializeEntryHelper(keys2[len(keys2)-1], "", false, true)

	// Create both SSTable formats to test
	CreateSeparatedSSTable(data1, lastKeyData1, 2, 2)
	CreateCompactSSTable(data2, lastKeyData2, 2, 2)

	t.Run("Many SSTables", func(t *testing.T) {
		for i, key := range keys1 {
			result := Find([]byte(key))
			checkNotNil(t, result, "Compact: missing result for "+key)
			checkEqual(t, string(result), values1[i], "Compact: wrong value for "+key)
		}

		for i, key := range keys2 {
			result := Find([]byte(key))
			checkNotNil(t, result, "Compact: missing result for "+key)
			checkEqual(t, string(result), values2[i], "Compact: wrong value for "+key)
		}
	})
}

func TestLargeKeyTombstoneCompact(t *testing.T) {
	os.RemoveAll("data")

	// Create a large key that spans multiple blocks
	largeKey := strings.Repeat("K", config.GlobalBlockSize*2)
	data := SerializeEntryHelper(largeKey, "", true, false)
	lastKeyData := SerializeEntryHelper(largeKey, "", true, true)

	// Write to compact SSTable
	CreateCompactSSTable(data, lastKeyData, 1, 1)

	t.Run("DeletedLargeKey", func(t *testing.T) {
		result := Find([]byte(largeKey))
		if result == nil || len(result) != 0 {
			t.Errorf("Expected an empty slice for large tombstoned key, got: %v", result)
		}
	})
}

func TestLargeKeyTombstoneSeparate(t *testing.T) {
	os.RemoveAll("data")

	// Create a large key that spans multiple blocks
	largeKey := strings.Repeat("K", config.GlobalBlockSize*2)
	data := SerializeEntryHelper(largeKey, "", true, false)
	lastKeyData := SerializeEntryHelper(largeKey, "", true, true)

	// Write to compact SSTable
	CreateSeparatedSSTable(data, lastKeyData, 1, 1)

	t.Run("DeletedLargeKey", func(t *testing.T) {
		result := Find([]byte(largeKey))
		if result == nil || len(result) != 0 {
			t.Errorf("Expected an empty slice for large tombstoned key, got: %v", result)
		}
	})
}

func TestTombstoneOverridesEarlierValues(t *testing.T) {
	os.RemoveAll("data")

	key := "conflictKey"

	// SSTable 1: key = "conflictKey", value = "value1"
	data1 := SerializeEntryHelper(key, "value1", false, false)
	last1 := SerializeEntryHelper(key, "", false, true)
	CreateSeparatedSSTable(data1, last1, 1, 1)

	// SSTable 2: key = "conflictKey", value = "value2"
	data2 := SerializeEntryHelper(key, "value2", false, false)
	last2 := SerializeEntryHelper(key, "", false, true)
	CreateCompactSSTable(data2, last2, 1, 1)

	// SSTable 3: key = "conflictKey", tombstone = true
	data3 := SerializeEntryHelper(key, "", true, false)
	last3 := SerializeEntryHelper(key, "", true, true)
	CreateCompactSSTable(data3, last3, 1, 1)

	// Check that the key is now treated as deleted
	t.Run("TombstoneTakesPrecedence", func(t *testing.T) {
		result := Find([]byte(key))
		if result == nil || len(result) != 0 {
			t.Errorf("Expected empty slice (deleted key), got: %v", result)
		}
	})
}
