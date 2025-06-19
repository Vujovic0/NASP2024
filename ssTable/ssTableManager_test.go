package ssTable

import (
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
	data := SerializeKeyValue(key, value, false, false)

	var block *channelResult
	for block = range PrepareSSTableBlocks(filePath, data, true, 0, false) {
		if block.Block == nil {
			t.Errorf("Expected block, got nil")
		}
		expectedCRC := crc32.ChecksumIEEE(block.Block.GetData()[4:])
		if binary.BigEndian.Uint32(block.Block.GetData()[0:4]) != expectedCRC {
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
		SerializeKeyValue("key1", "value1", false, false),
		SerializeKeyValue("key2", "value2", false, false),
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
	data := SerializeKeyValue(key, value, false, false)

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
	data := SerializeKeyValue(key, value, false, false)

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
	data := SerializeKeyValue(key, value, false, false)
	lastKeyData := SerializeKeyValue(key, "", false, true)

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

func TestTombstoneHandlingSepareted(t *testing.T) {
	os.RemoveAll("data")

	key := "deletedKey"
	data := SerializeKeyValue(key, "", true, false)
	lastKeyData := SerializeKeyValue(key, "", true, true)

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
	data := SerializeKeyValue(key, "", true, false)
	lastKeyData := SerializeKeyValue(key, "", true, true)

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
		bigData = append(bigData, SerializeKeyValue(key, value, false, false)...)
	}
	lastKeyData := SerializeKeyValue(fmt.Sprintf("key%04d", 9), "", false, true)

	CreateCompactSSTable(bigData, lastKeyData, 10, 10)

	t.Run("RandomAccess", func(t *testing.T) {
		for i := 0; i < 4; i++ {
			testKey := fmt.Sprintf("key%04d", i*3)
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
		bigData = append(bigData, SerializeKeyValue(key, value, false, false)...)
	}
	lastKeyData := SerializeKeyValue(fmt.Sprintf("key%04d", 9), "", false, true)

	CreateSeparatedSSTable(bigData, lastKeyData, 5, 10)

	t.Run("RandomAccess", func(t *testing.T) {
		for i := 0; i < 4; i++ {
			testKey := fmt.Sprintf("key%04d", i*3)
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
		data = append(data, SerializeKeyValue(key, value, false, false)...)
	}
	lastKeyData := SerializeKeyValue("key0030", "", false, true)

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
