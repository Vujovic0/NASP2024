package ssTable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Vujovic0/NASP2024/config"
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
		result := SearchAll([]byte(key), false)
		checkNotNil(t, result, "Should find value in separated SSTable")
		checkEqual(t, string(result), value, "Incorrect value in separated SSTable")
	})

	// Test compact SSTable
	t.Run("Compact SSTable", func(t *testing.T) {
		CreateCompactSSTable(data, lastKeyData, 1, 1)
		result := SearchAll([]byte(key), false)
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
	result := SearchAll([]byte(key), false)

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
	result := SearchAll([]byte(key), false)

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
			result := SearchAll([]byte(testKey), false)
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
			result := SearchAll([]byte(testKey), false)
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
			result := SearchAll([]byte(tc.key), false)
			if result != nil && len(result) != 0 {
				t.Errorf("Compact - %s: expected nil or empty, got: %v", tc.name, result)
			}
		}
	})

	t.Run("Separated SSTable", func(t *testing.T) {
		for _, tc := range testCases {
			result := SearchAll([]byte(tc.key), false)
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
			result := SearchAll([]byte(key), false)
			checkNotNil(t, result, "Compact: missing result for "+key)
			checkEqual(t, string(result), values[i], "Compact: wrong value for "+key)
		}
	})

	t.Run("Separated SSTable", func(t *testing.T) {
		for i, key := range keys {
			result := SearchAll([]byte(key), false)
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
			result := SearchAll([]byte(testKey), false)
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
			result := SearchAll([]byte(key), false)
			checkNotNil(t, result, "Compact: missing result for "+key)
			checkEqual(t, string(result), values1[i], "Compact: wrong value for "+key)
		}

		for i, key := range keys2 {
			result := SearchAll([]byte(key), false)
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
		result := SearchAll([]byte(largeKey), false)
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
		result := SearchAll([]byte(largeKey), false)
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
		result := SearchAll([]byte(key), false)
		if result == nil || len(result) != 0 {
			t.Errorf("Expected empty slice (deleted key), got: %v", result)
		}
	})
}
func TestFindLastSmallerKey_NoPrefix(t *testing.T) {
	keys := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("carrot"),
		[]byte("date"),
		[]byte("fig"),
	}

	tests := []struct {
		name      string
		key       []byte
		dataBlock bool
		want      int64
	}{
		// Exact matches
		{"ExactMatchFirst", []byte("apple"), false, 0},
		{"ExactMatchMiddle", []byte("carrot"), false, 2},
		{"ExactMatchLast", []byte("fig"), false, 4},

		// Key smaller than first
		{"SmallerThanFirst_DataBlockFalse", []byte("bardvark"), false, 1},
		{"SmallerThanFirst_DataBlockTrue", []byte("bardvark"), true, -2},

		// Key greater than all
		{"GreaterThanLast", []byte("grape"), false, -1},

		// Key between elements
		{"BetweenBananaAndCarrot_DataBlockFalse", []byte("blueberry"), false, 1},
		{"BetweenBananaAndCarrot_DataBlockTrue", []byte("blueberry"), true, -2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FindLastSmallerKey(tt.key, keys, tt.dataBlock, false) // prefix = false always
			if got != tt.want {
				t.Errorf("FindLastSmallerKey(%q, dataBlock=%v) = %d; want %d", tt.key, tt.dataBlock, got, tt.want)
			}
		})
	}
}

func TestSearchAllPrefix(t *testing.T) {
	os.RemoveAll("data")

	// Test podaci
	keys := []string{
		"apple",
		"banana",
		"bananaPie",
		"carrot",
		"carrotCake",
		"date",
		"fig",
	}
	values := []string{
		"val_apple",
		"val_banana",
		"val_bananaPie",
		"val_carrot",
		"val_carrotCake",
		"val_date",
		"val_fig",
	}

	// Kreiranje SSTabele
	var data []byte
	for i := range keys {
		data = append(data, SerializeEntryHelper(keys[i], values[i], false, false)...)
	}
	lastKey := SerializeEntryHelper(keys[len(keys)-1], "", false, true)
	CreateCompactSSTable(data, lastKey, len(keys), len(keys))

	// Test slučajevi
	tests := []struct {
		name     string
		prefix   []byte
		expected string
		found    bool
	}{
		{"Prefix 'ban'", []byte("ban"), "val_banana", true},
		{"Prefix 'bananaP'", []byte("bananaP"), "val_bananaPie", true},
		{"Prefix 'carrotC'", []byte("carrotC"), "val_carrotCake", true},
		{"Prefix 'f'", []byte("f"), "val_fig", true},
		{"Prefix 'x'", []byte("x"), "", false},
		{"Prefix 'ap'", []byte("ap"), "val_apple", true},
		{"Prefix '' (empty)", []byte(""), "val_apple", true},
	}

	// Izvršavanje testova
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			value := SearchAll(tc.prefix, true)
			if tc.found {
				checkNotNil(t, value, "Expected match for prefix "+string(tc.prefix))
				checkEqual(t, string(value), tc.expected, "Incorrect value for prefix "+string(tc.prefix))
			} else {
				if value != nil {
					t.Errorf("Expected nil for prefix %q, got: %v", tc.prefix, value)
				}
			}
		})
	}
}

// | CRC 4B | TimeStamp 8B | Tombstone 1B | Keysize 8B | Valuesize 8B | Key... | Value... |
// KeyOnlyCheck will assure that only the key with its size is serialized
func SerializeEntryHelper(key string, value string, tombstone bool, keyOnly bool) []byte {
	if !config.VariableEncoding {
		keyBytes := []byte(key)
		keySize := len(keyBytes)
		var dataBytes []byte
		if keyOnly {
			dataBytes = make([]byte, 8+keySize)
			binary.LittleEndian.PutUint64(dataBytes[0:8], uint64(keySize))
			copy(dataBytes[8:], keyBytes)
			return dataBytes
		}
		valueBytes := []byte(value)
		valueSize := len(valueBytes)
		timestamp := time.Now().Unix()
		sizeReserve := 4 + 8 + 1 + 8 + 8 + keySize + valueSize
		if tombstone {
			sizeReserve -= valueSize + 8
		}
		dataBytes = make([]byte, sizeReserve)
		binary.LittleEndian.PutUint64(dataBytes[4:12], uint64(timestamp))
		if tombstone {
			dataBytes[12] = 1
		}
		binary.LittleEndian.PutUint64(dataBytes[13:21], uint64(keySize))
		if !tombstone {
			binary.LittleEndian.PutUint64(dataBytes[21:29], uint64(valueSize))
			copy(dataBytes[29+keySize:], valueBytes)
			copy(dataBytes[29:29+keySize], keyBytes)
		} else {
			copy(dataBytes[21:21+keySize], keyBytes)
		}
		crc := crc32.ChecksumIEEE(dataBytes[4:])
		binary.LittleEndian.PutUint32(dataBytes[:4], crc)
		return dataBytes
	} else {
		data := make([]byte, 0)
		if keyOnly {
			keyBytes := []byte(key)
			data = binary.AppendUvarint(data, uint64(len(keyBytes)))
			data = append(data, keyBytes...)
			return data
		}
		//make space for crc32
		data = append(data, []byte{0, 0, 0, 0}...)
		timestamp := time.Now().Unix()
		keyBytes := []byte(key)
		valueBytes := []byte(value)
		//append timestamp data
		data = binary.AppendUvarint(data, uint64(timestamp))
		//append tombstone
		if tombstone {
			data = binary.AppendUvarint(data, 1)
		} else {
			data = binary.AppendUvarint(data, 0)
		}
		//append key size
		data = binary.AppendUvarint(data, uint64(len(keyBytes)))
		//apend value size
		if !tombstone {
			data = binary.AppendUvarint(data, uint64(len(valueBytes)))
		}
		//append key
		data = append(data, keyBytes...)
		//append value
		data = append(data, valueBytes...)

		crc := crc32.ChecksumIEEE(data[4:])
		//fill reserved data with crc32
		binary.LittleEndian.PutUint32(data[0:4], crc)
		return data
	}
}
