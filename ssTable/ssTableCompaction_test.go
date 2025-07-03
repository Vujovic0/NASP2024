package ssTable

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCompactionBasicMerge(t *testing.T) {
	os.RemoveAll("data")

	// Table 1
	data1 := SerializeEntryHelper("a", "1", false, false)
	data1 = append(data1, SerializeEntryHelper("b", "2", false, false)...)
	last1 := SerializeEntryHelper("b", "", false, true)
	CreateSeparatedSSTable(data1, last1, 2, 2)

	// Table 2
	data2 := SerializeEntryHelper("c", "3", false, false)
	last2 := SerializeEntryHelper("c", "", false, true)
	CreateSeparatedSSTable(data2, last2, 1, 1)

	files := openAllDataFiles(t)
	folderPath := filepath.Join("data", "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join("data", "L1", "usertable-3-compact.bin")
	MergeTables(files, newFilePath)
	cleanUpOldFiles(t, files)

	result := SearchAll([]byte("a"), false)
	checkEqual(t, string(result), "1", "Incorrect value after compaction")

	result = SearchAll([]byte("b"), false)
	checkEqual(t, string(result), "2", "Incorrect value after compaction")

	result = SearchAll([]byte("c"), false)
	checkEqual(t, string(result), "3", "Incorrect value after compaction")
}

func TestCompactionWithOverlappingKeys(t *testing.T) {
	os.RemoveAll("data")

	// Table 1
	data1 := SerializeEntryHelper("a", "1", false, false)
	data1 = append(data1, SerializeEntryHelper("b", "2", false, false)...)
	last1 := SerializeEntryHelper("b", "", false, true)
	CreateSeparatedSSTable(data1, last1, 2, 2)
	time.Sleep(1 * time.Second)
	// Table 2 (overwrite "b")
	data2 := SerializeEntryHelper("b", "9", false, false)
	data2 = append(data2, SerializeEntryHelper("c", "3", false, false)...)
	last2 := SerializeEntryHelper("c", "", false, true)
	CreateSeparatedSSTable(data2, last2, 2, 2)

	files := openAllDataFiles(t)
	folderPath := filepath.Join("data", "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join("data", "L1", "usertable-3-compact.bin")
	MergeTables(files, newFilePath)
	cleanUpOldFiles(t, files)

	checkEqual(t, string(SearchAll([]byte("a"), false)), "1", "Expected a=1")
	checkEqual(t, string(SearchAll([]byte("b"), false)), "9", "Expected b=9 from newer table")
	checkEqual(t, string(SearchAll([]byte("c"), false)), "3", "Expected c=3")
}

func TestCompactionWithEmptyTable(t *testing.T) {
	os.RemoveAll("data")

	// Table 1: contains only tombstoned key
	data1 := SerializeEntryHelper("x", "", true, false)
	last1 := SerializeEntryHelper("x", "", true, true)
	CreateSeparatedSSTable(data1, last1, 1, 1)

	// Table 2: contains only tombstoned key
	data2 := SerializeEntryHelper("b", "", true, false)
	last2 := SerializeEntryHelper("b", "", true, true)
	CreateSeparatedSSTable(data2, last2, 1, 1)

	// Perform compaction
	files := openAllDataFiles(t)
	folderPath := filepath.Join("data", "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join(folderPath, "usertable-3-compact.bin")
	MergeTables(files, newFilePath)
	cleanUpOldFiles(t, files)

	// ✅ Assert that new merged file does not exist (compaction result was empty)
	if _, err := os.Stat(newFilePath); !os.IsNotExist(err) {
		t.Errorf("Expected no merged file to be created, but %s exists", newFilePath)
	}

	// ✅ Assert that original SSTables are deleted
	entries, err := os.ReadDir("data")
	if err != nil {
		t.Fatalf("Failed to read data directory: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "usertable") {
			t.Errorf("Expected original table %s to be deleted after compaction", entry.Name())
		}
	}
}

func TestCompactionMultiBlockEntries(t *testing.T) {
	os.RemoveAll("data")

	largeValue := strings.Repeat("X", int(blockSize)*2) // Force value to span 2 blocks

	// Table 1
	data1 := SerializeEntryHelper("a", largeValue, false, false)
	last1 := SerializeEntryHelper("a", "", false, true)
	CreateSeparatedSSTable(data1, last1, 1, 1)

	time.Sleep(1 * time.Second)
	// Table 2
	data2 := SerializeEntryHelper("b", largeValue, false, false)
	last2 := SerializeEntryHelper("b", "", false, true)
	CreateSeparatedSSTable(data2, last2, 1, 1)

	files := openAllDataFiles(t)
	folderPath := filepath.Join("data", "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join(folderPath, "usertable-3-compact.bin")
	MergeTables(files, newFilePath)
	cleanUpOldFiles(t, files)

	resultA := SearchAll([]byte("a"), false)
	checkEqual(t, len(resultA), len(largeValue), "Length mismatch for key a")
	checkEqual(t, string(resultA), largeValue, "Incorrect value for key a")

	resultB := SearchAll([]byte("b"), false)
	checkEqual(t, len(resultB), len(largeValue), "Length mismatch for key b")
	checkEqual(t, string(resultB), largeValue, "Incorrect value for key b")
}

func TestCompactionMultiBlockKeysCompact(t *testing.T) {
	os.RemoveAll("data")

	// Make a large key that spans more than one block
	largeKey := strings.Repeat("K", int(blockSize)*2)
	value := "short-val"

	// Table 1
	data1 := SerializeEntryHelper(largeKey, value, false, false)
	last1 := SerializeEntryHelper(largeKey, "", false, true)
	CreateSeparatedSSTable(data1, last1, 1, 1)

	time.Sleep(1 * time.Second)

	// Table 2 with a second large key
	largeKey2 := strings.Repeat("Q", int(blockSize)*2)
	data2 := SerializeEntryHelper(largeKey2, value, false, false)
	last2 := SerializeEntryHelper(largeKey2, "", false, true)
	CreateSeparatedSSTable(data2, last2, 1, 1)

	// Merge tables
	files := openAllDataFiles(t)
	folderPath := filepath.Join("data", "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join(folderPath, "usertable-3-compact.bin")
	MergeTables(files, newFilePath)
	cleanUpOldFiles(t, files)

	// Verify correct retrieval of both large keys
	result1 := SearchAll([]byte(largeKey), false)
	checkEqual(t, string(result1), value, "Incorrect value for large key 1")

	result2 := SearchAll([]byte(largeKey2), false)
	checkEqual(t, string(result2), value, "Incorrect value for large key 2")
}

func TestCompactionMultiBlockKeysSeparate(t *testing.T) {
	os.RemoveAll("data")

	// Make a large key that spans more than one block
	largeKey := strings.Repeat("K", int(blockSize)*2)
	value := "short-val"

	// Table 1
	data1 := SerializeEntryHelper(largeKey, value, false, false)
	last1 := SerializeEntryHelper(largeKey, "", false, true)
	CreateSeparatedSSTable(data1, last1, 1, 1)

	time.Sleep(1 * time.Second)

	// Table 2 with a second large key
	largeKey2 := strings.Repeat("Q", int(blockSize)*2)
	data2 := SerializeEntryHelper(largeKey2, value, false, false)
	last2 := SerializeEntryHelper(largeKey2, "", false, true)
	CreateSeparatedSSTable(data2, last2, 1, 1)

	// Merge tables
	files := openAllDataFiles(t)
	folderPath := filepath.Join("data", "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join(folderPath, "usertable-3-data.bin")
	MergeTables(files, newFilePath)
	cleanUpOldFiles(t, files)

	// Verify correct retrieval of both large keys
	result1 := SearchAll([]byte(largeKey), false)
	checkEqual(t, string(result1), value, "Incorrect value for large key 1")

	result2 := SearchAll([]byte(largeKey2), false)
	checkEqual(t, string(result2), value, "Incorrect value for large key 2")
}

func TestCompactionLargeKeyLargeValueSeparate(t *testing.T) {
	os.RemoveAll("data")

	// Make a large key that spans more than one block
	largeKey := strings.Repeat("K", int(blockSize)*2)
	value := strings.Repeat("X", int(blockSize)*2)

	// Table 1
	data1 := SerializeEntryHelper(largeKey, value, false, false)
	last1 := SerializeEntryHelper(largeKey, "", false, true)
	CreateSeparatedSSTable(data1, last1, 1, 1)

	time.Sleep(1 * time.Second)

	// Table 2 with a second large key
	largeKey2 := strings.Repeat("Q", int(blockSize)*2)
	data2 := SerializeEntryHelper(largeKey2, value, false, false)
	last2 := SerializeEntryHelper(largeKey2, "", false, true)
	CreateSeparatedSSTable(data2, last2, 1, 1)

	// Merge tables
	files := openAllDataFiles(t)
	folderPath := filepath.Join("data", "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join(folderPath, "usertable-3-data.bin")
	MergeTables(files, newFilePath)
	cleanUpOldFiles(t, files)

	// Verify correct retrieval of both large keys
	result1 := SearchAll([]byte(largeKey), false)
	checkEqual(t, string(result1), value, "Incorrect value for large key 1")

	result2 := SearchAll([]byte(largeKey2), false)
	checkEqual(t, string(result2), value, "Incorrect value for large key 2")
}

func TestCompactionLargeKeyLargeValueCompact(t *testing.T) {
	os.RemoveAll("data")

	// Make a large key that spans more than one block
	largeKey := strings.Repeat("K", int(blockSize)*2)
	value := strings.Repeat("X", int(blockSize)*2)

	// Table 1
	data1 := SerializeEntryHelper(largeKey, value, false, false)
	last1 := SerializeEntryHelper(largeKey, "", false, true)
	CreateSeparatedSSTable(data1, last1, 1, 1)

	time.Sleep(1 * time.Second)

	// Table 2 with a second large key
	largeKey2 := strings.Repeat("Q", int(blockSize)*2)
	data2 := SerializeEntryHelper(largeKey2, value, false, false)
	last2 := SerializeEntryHelper(largeKey2, "", false, true)
	CreateSeparatedSSTable(data2, last2, 1, 1)

	// Merge tables
	files := openAllDataFiles(t)
	folderPath := filepath.Join("data", "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join(folderPath, "usertable-3-compact.bin")
	MergeTables(files, newFilePath)
	cleanUpOldFiles(t, files)

	// Verify correct retrieval of both large keys
	result1 := SearchAll([]byte(largeKey), false)
	checkEqual(t, string(result1), value, "Incorrect value for large key 1")

	result2 := SearchAll([]byte(largeKey2), false)
	checkEqual(t, string(result2), value, "Incorrect value for large key 2")
}

func TestCompactionMixedSizesCompact(t *testing.T) {
	os.RemoveAll("data")

	// Large entries
	largeKey1 := strings.Repeat("K", int(blockSize)*2)
	largeKey2 := strings.Repeat("Q", int(blockSize)*2)
	largeValue := strings.Repeat("X", int(blockSize)*2)

	// Small entries
	smallKey1 := "apple"
	smallKey2 := "banana"
	smallValue := "fruit"

	// --- Table 1: One large and one small entry ---
	data1 := SerializeEntryHelper(largeKey1, largeValue, false, false)
	data1 = append(data1, SerializeEntryHelper(smallKey1, smallValue, false, false)...)
	last1 := SerializeEntryHelper(smallKey1, "", false, true) // last key marker
	CreateSeparatedSSTable(data1, last1, 1, 1)

	time.Sleep(1 * time.Second)

	// --- Table 2: Another large and another small entry ---
	data2 := SerializeEntryHelper(largeKey2, largeValue, false, false)
	data2 = append(data2, SerializeEntryHelper(smallKey2, smallValue, false, false)...)
	last2 := SerializeEntryHelper(smallKey2, "", false, true)
	CreateSeparatedSSTable(data2, last2, 1, 1)

	// --- Perform compaction ---
	files := openAllDataFiles(t)
	folderPath := filepath.Join("data", "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join(folderPath, "usertable-3-compact.bin")
	MergeTables(files, newFilePath)
	cleanUpOldFiles(t, files)

	// --- Validate all entries after compaction ---
	result1 := SearchAll([]byte(largeKey1), false)
	checkEqual(t, string(result1), largeValue, "Incorrect value for large key 1")

	result2 := SearchAll([]byte(largeKey2), false)
	checkEqual(t, string(result2), largeValue, "Incorrect value for large key 2")

	result3 := SearchAll([]byte(smallKey1), false)
	checkEqual(t, string(result3), smallValue, "Incorrect value for small key 1")

	result4 := SearchAll([]byte(smallKey2), false)
	checkEqual(t, string(result4), smallValue, "Incorrect value for small key 2")
}

func TestCompactionMixedSizesSeparate(t *testing.T) {
	os.RemoveAll("data")

	// Large entries
	largeKey1 := strings.Repeat("K", int(blockSize)*2)
	largeKey2 := strings.Repeat("Q", int(blockSize)*2)
	largeValue := strings.Repeat("X", int(blockSize)*2)

	// Small entries
	smallKey1 := "apple"
	smallKey2 := "banana"
	smallValue := "fruit"

	// --- Table 1: One large and one small entry ---
	data1 := SerializeEntryHelper(largeKey1, largeValue, false, false)
	data1 = append(data1, SerializeEntryHelper(smallKey1, smallValue, false, false)...)
	last1 := SerializeEntryHelper(smallKey1, "", false, true) // last key marker
	CreateSeparatedSSTable(data1, last1, 1, 1)

	time.Sleep(1 * time.Second)

	// --- Table 2: Another large and another small entry ---
	data2 := SerializeEntryHelper(largeKey2, largeValue, false, false)
	data2 = append(data2, SerializeEntryHelper(smallKey2, smallValue, false, false)...)
	last2 := SerializeEntryHelper(smallKey2, "", false, true)
	CreateSeparatedSSTable(data2, last2, 1, 1)

	// --- Perform compaction ---
	files := openAllDataFiles(t)
	folderPath := filepath.Join("data", "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join(folderPath, "usertable-3-data.bin")
	MergeTables(files, newFilePath)
	cleanUpOldFiles(t, files)

	// --- Validate all entries after compaction ---
	result1 := SearchAll([]byte(largeKey1), false)
	checkEqual(t, string(result1), largeValue, "Incorrect value for large key 1")

	result2 := SearchAll([]byte(largeKey2), false)
	checkEqual(t, string(result2), largeValue, "Incorrect value for large key 2")

	result3 := SearchAll([]byte(smallKey1), false)
	checkEqual(t, string(result3), smallValue, "Incorrect value for small key 1")

	result4 := SearchAll([]byte(smallKey2), false)
	checkEqual(t, string(result4), smallValue, "Incorrect value for small key 2")
}

func TestCompactionWithManyOverlappingKeys(t *testing.T) {
	os.RemoveAll("data")

	// Table 1: keys a to e
	data1 := []byte{}
	data1 = append(data1, SerializeEntryHelper("a", "1", false, false)...)
	data1 = append(data1, SerializeEntryHelper("b", "2", false, false)...)
	data1 = append(data1, SerializeEntryHelper("c", "3", false, false)...)
	data1 = append(data1, SerializeEntryHelper("d", "4", false, false)...)
	data1 = append(data1, SerializeEntryHelper("e", "5", false, false)...)
	last1 := SerializeEntryHelper("e", "", false, true)
	CreateSeparatedSSTable(data1, last1, 5, 2)
	time.Sleep(1 * time.Second)

	// Table 2: overlapping keys b to g, some updated, some new
	data2 := []byte{}
	data2 = append(data2, SerializeEntryHelper("b", "20", false, false)...)
	data2 = append(data2, SerializeEntryHelper("c", "30", false, false)...)
	data2 = append(data2, SerializeEntryHelper("f", "6", false, false)...)
	data2 = append(data2, SerializeEntryHelper("g", "7", false, false)...)
	last2 := SerializeEntryHelper("g", "", false, true)
	CreateSeparatedSSTable(data2, last2, 4, 2)
	time.Sleep(1 * time.Second)

	// Table 3: overlapping keys a, d, h with updates and new
	data3 := []byte{}
	data3 = append(data3, SerializeEntryHelper("a", "100", false, false)...)
	data3 = append(data3, SerializeEntryHelper("d", "400", false, false)...)
	data3 = append(data3, SerializeEntryHelper("h", "8", false, false)...)
	last3 := SerializeEntryHelper("h", "", false, true)
	CreateSeparatedSSTable(data3, last3, 3, 2)
	time.Sleep(1 * time.Second)

	files := openAllDataFiles(t)
	folderPath := filepath.Join("data", "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join("data", "L1", "usertable-3-compact.bin")
	MergeTables(files, newFilePath)
	cleanUpOldFiles(t, files)

	// Check expected values after compaction (newest timestamp wins)
	checkEqual(t, string(SearchAll([]byte("a"), false)), "100", "Expected a=100 (from table 3)")
	checkEqual(t, string(SearchAll([]byte("b"), false)), "20", "Expected b=20 (from table 2)")
	checkEqual(t, string(SearchAll([]byte("c"), false)), "30", "Expected c=30 (from table 2)")
	checkEqual(t, string(SearchAll([]byte("d"), false)), "400", "Expected d=400 (from table 3)")
	checkEqual(t, string(SearchAll([]byte("e"), false)), "5", "Expected e=5 (from table 1)")
	checkEqual(t, string(SearchAll([]byte("f"), false)), "6", "Expected f=6 (from table 2)")
	checkEqual(t, string(SearchAll([]byte("g"), false)), "7", "Expected g=7 (from table 2)")
	checkEqual(t, string(SearchAll([]byte("h"), false)), "8", "Expected h=8 (from table 3)")
	checkEqual(t, string(SearchAll([]byte("z"), false)), "", "Expected z to not be found (non-existent key)")
	checkEqual(t, string(SearchAll([]byte("3"), false)), "", "Expected 3 to not be found (non-existent key)")
	checkEqual(t, string(SearchAll([]byte("c1"), false)), "", "Expected c1 to not be found (non-existent key)")
	checkEqual(t, string(SearchAll([]byte("abcde"), false)), "", "Expected abcde to not be found (non-existent key)")
}

func TestCompactionNewerKeysOverrideOlder(t *testing.T) {
	os.RemoveAll("data")

	// Table 1: older values
	data1 := []byte{}
	data1 = append(data1, SerializeEntryHelper("key1", "old_value1", false, false)...)
	data1 = append(data1, SerializeEntryHelper("key2", "old_value2", false, false)...)
	last1 := SerializeEntryHelper("key2", "", false, true)
	CreateSeparatedSSTable(data1, last1, 2, 2)

	// Wait a bit so next SSTable has newer timestamps
	time.Sleep(1 * time.Second)

	// Table 2: newer values overwrite some keys
	data2 := []byte{}
	data2 = append(data2, SerializeEntryHelper("key1", "new_value1", false, false)...)
	data2 = append(data2, SerializeEntryHelper("key3", "new_value3", false, false)...)
	last2 := SerializeEntryHelper("key3", "", false, true)
	CreateSeparatedSSTable(data2, last2, 2, 2)

	files := openAllDataFiles(t)
	folderPath := filepath.Join("data", "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join("data", "L1", "usertable-3-compact.bin")
	MergeTables(files, newFilePath)

	time.Sleep(1 * time.Second)
	CreateSeparatedSSTable(data1, last1, 2, 2)

	// Verify that after compaction, Find returns newest values
	checkEqual(t, string(SearchAll([]byte("key1"), false)), "old_value1", "Expected key1 to have older value")
	checkEqual(t, string(SearchAll([]byte("key2"), false)), "old_value2", "Expected key2 to keep old value (no overwrite)")
	checkEqual(t, string(SearchAll([]byte("key3"), false)), "new_value3", "Expected key3 to have new value")
	cleanUpOldFiles(t, files)
}

func openAllDataFiles(t *testing.T) []*os.File {
	t.Helper()
	pathSep := string(os.PathSeparator)
	L0Path := "data" + pathSep + "L0"
	dirEntries, err := os.ReadDir(L0Path)
	if err != nil {
		t.Fatalf("Failed to read data dir: %v", err)
	}
	var files []*os.File
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), "data.bin") {
			file, err := os.Open(L0Path + pathSep + entry.Name())
			if err != nil {
				t.Fatalf("Failed to open file: %v", err)
			}
			files = append(files, file)
		}
	}
	return files
}

func closeAllDataFiles(t *testing.T, files []*os.File) {
	t.Helper()
	for _, file := range files {
		file.Close()
	}
}

func deleteAllDataFiles(t *testing.T) {
	t.Helper()
	filePath := filepath.Join("data", "L0")
	os.RemoveAll(filePath)
}

func cleanUpOldFiles(t *testing.T, files []*os.File) {
	closeAllDataFiles(t, files)
	deleteAllDataFiles(t)
}
