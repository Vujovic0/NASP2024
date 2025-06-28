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

	result := Find([]byte("a"))
	checkEqual(t, string(result), "1", "Incorrect value after compaction")

	result = Find([]byte("b"))
	checkEqual(t, string(result), "2", "Incorrect value after compaction")

	result = Find([]byte("c"))
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

	checkEqual(t, string(Find([]byte("a"))), "1", "Expected a=1")
	checkEqual(t, string(Find([]byte("b"))), "9", "Expected b=9 from newer table")
	checkEqual(t, string(Find([]byte("c"))), "3", "Expected c=3")
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

	resultA := Find([]byte("a"))
	checkEqual(t, len(resultA), len(largeValue), "Length mismatch for key a")
	checkEqual(t, string(resultA), largeValue, "Incorrect value for key a")

	resultB := Find([]byte("b"))
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
	result1 := Find([]byte(largeKey))
	checkEqual(t, string(result1), value, "Incorrect value for large key 1")

	result2 := Find([]byte(largeKey2))
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
	result1 := Find([]byte(largeKey))
	checkEqual(t, string(result1), value, "Incorrect value for large key 1")

	result2 := Find([]byte(largeKey2))
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
	result1 := Find([]byte(largeKey))
	checkEqual(t, string(result1), value, "Incorrect value for large key 1")

	result2 := Find([]byte(largeKey2))
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
	result1 := Find([]byte(largeKey))
	checkEqual(t, string(result1), value, "Incorrect value for large key 1")

	result2 := Find([]byte(largeKey2))
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
	result1 := Find([]byte(largeKey1))
	checkEqual(t, string(result1), largeValue, "Incorrect value for large key 1")

	result2 := Find([]byte(largeKey2))
	checkEqual(t, string(result2), largeValue, "Incorrect value for large key 2")

	result3 := Find([]byte(smallKey1))
	checkEqual(t, string(result3), smallValue, "Incorrect value for small key 1")

	result4 := Find([]byte(smallKey2))
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
	result1 := Find([]byte(largeKey1))
	checkEqual(t, string(result1), largeValue, "Incorrect value for large key 1")

	result2 := Find([]byte(largeKey2))
	checkEqual(t, string(result2), largeValue, "Incorrect value for large key 2")

	result3 := Find([]byte(smallKey1))
	checkEqual(t, string(result3), smallValue, "Incorrect value for small key 1")

	result4 := Find([]byte(smallKey2))
	checkEqual(t, string(result4), smallValue, "Incorrect value for small key 2")
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
