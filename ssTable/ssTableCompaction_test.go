package ssTable

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func openAllDataFiles(t *testing.T) []*os.File {
	t.Helper()
	pathSep := string(os.PathSeparator)
	dataPath := getDataPath()
	L0Path := dataPath + pathSep + "L0"
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
		} else if strings.HasSuffix(entry.Name(), "compact.bin") {
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
	dataPath := getDataPath()
	t.Helper()
	filePath := filepath.Join(dataPath, "L0")
	os.RemoveAll(filePath)
}

func cleanUpOldFiles(t *testing.T, files []*os.File) {
	closeAllDataFiles(t, files)
	deleteAllDataFiles(t)
}

func TestCompactionBasicMerge(t *testing.T) {
	dataPath := getDataPath()
	os.RemoveAll(dataPath)

	// Table 1
	data1 := SerializeEntryHelper("a", "1", false, false)
	data1 = append(data1, SerializeEntryHelper("b", "2", false, false)...)
	last1 := SerializeEntryHelper("b", "", false, true)

	crc32_1 := getHashes([][]byte{SerializeEntryHelper("a", "1", false, false), SerializeEntryHelper("b", "2", false, false)})
	filter_1 := createFilter([]string{"a", "b"})
	tree_1 := createTree(crc32_1)
	CreateCompactSSTable(data1, last1, 2, 2, filter_1, tree_1)

	// Table 2
	data2 := SerializeEntryHelper("c", "3", false, false)
	last2 := SerializeEntryHelper("c", "", false, true)

	crc32_2 := getHashes([][]byte{data2})
	filter_2 := createFilter([]string{"c"})
	tree_2 := createTree(crc32_2)
	CreateCompactSSTable(data2, last2, 1, 1, filter_2, tree_2)

	files := openAllDataFiles(t)
	folderPath := filepath.Join(dataPath, "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join(dataPath, "L1", "usertable-3-compact.bin")
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
	dataPath := getDataPath()
	os.RemoveAll(dataPath)

	// Table 1
	data1 := SerializeEntryHelper("a", "1", false, false)
	data1 = append(data1, SerializeEntryHelper("b", "2", false, false)...)
	last1 := SerializeEntryHelper("b", "", false, true)

	// Kreiraj Bloom filter i CRC32 stablo za prvu tabelu
	crc32_1 := getHashes([][]byte{
		SerializeEntryHelper("a", "1", false, false),
		SerializeEntryHelper("b", "2", false, false),
	})
	filter_1 := createFilter([]string{"a", "b"})
	tree_1 := createTree(crc32_1)
	CreateCompactSSTable(data1, last1, 2, 2, filter_1, tree_1)
	time.Sleep(1 * time.Second)

	// Table 2 (overwrite "b")
	data2 := SerializeEntryHelper("b", "9", false, false)
	data2 = append(data2, SerializeEntryHelper("c", "3", false, false)...)
	last2 := SerializeEntryHelper("c", "", false, true)

	// Kreiraj Bloom filter i CRC32 stablo za drugu tabelu
	crc32_2 := getHashes([][]byte{
		SerializeEntryHelper("b", "9", false, false),
		SerializeEntryHelper("c", "3", false, false),
	})
	filter_2 := createFilter([]string{"b", "c"})
	tree_2 := createTree(crc32_2)
	CreateCompactSSTable(data2, last2, 2, 2, filter_2, tree_2)

	// Proces kompakcije
	files := openAllDataFiles(t)
	folderPath := filepath.Join(dataPath, "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join(dataPath, "L1", "usertable-3-compact.bin")
	MergeTables(files, newFilePath)
	cleanUpOldFiles(t, files)

	// Provera rezultata
	checkEqual(t, string(SearchAll([]byte("a"), false)), "1", "Expected a=1")
	checkEqual(t, string(SearchAll([]byte("b"), false)), "9", "Expected b=9 from newer table")
	checkEqual(t, string(SearchAll([]byte("c"), false)), "3", "Expected c=3")
	checkEqual(t, string(SearchAll([]byte("d"), false)), []byte{}, "Expected d=[]")
}

func TestCompactionWithEmptyTable(t *testing.T) {
	dataPath := getDataPath()
	os.RemoveAll(dataPath)

	// Table 1: contains only tombstoned key
	data1 := SerializeEntryHelper("x", "", true, false)
	last1 := SerializeEntryHelper("x", "", true, true)

	// Kreiraj Bloom filter i CRC32 stablo za prvu tabelu (prazan filter za tombstone)
	crc32_1 := getHashes([][]byte{data1})
	filter_1 := createFilter([]string{"x"}) // iako je tombstone, ključ se i dalje dodaje u filter
	tree_1 := createTree(crc32_1)
	CreateCompactSSTable(data1, last1, 1, 1, filter_1, tree_1)

	// Table 2: contains only tombstoned key
	data2 := SerializeEntryHelper("b", "", true, false)
	last2 := SerializeEntryHelper("b", "", true, true)

	// Kreiraj Bloom filter i CRC32 stablo za drugu tabelu
	crc32_2 := getHashes([][]byte{data2})
	filter_2 := createFilter([]string{"b"})
	tree_2 := createTree(crc32_2)
	CreateCompactSSTable(data2, last2, 1, 1, filter_2, tree_2)

	// Perform compaction
	files := openAllDataFiles(t)
	folderPath := filepath.Join(dataPath, "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join(folderPath, "usertable-3-compact.bin")
	MergeTables(files, newFilePath)

	// ✅ Assert that new merged file does not exist (compaction result was empty)
	if _, err := os.Stat(newFilePath); !os.IsNotExist(err) {
		t.Errorf("Expected no merged file to be created, but %s exists", newFilePath)
	}

	cleanUpOldFiles(t, files)
}

func TestCompactionMultiBlockEntries(t *testing.T) {
	dataPath := getDataPath()
	os.RemoveAll(dataPath)

	largeValue := strings.Repeat("X", int(blockSize)*2) // Force value to span 2 blocks

	// Table 1
	data1 := SerializeEntryHelper("a", largeValue, false, false)
	last1 := SerializeEntryHelper("a", "", false, true)

	// Create Bloom filter and CRC32 tree for table 1
	crc32_1 := getHashes([][]byte{data1})
	filter_1 := createFilter([]string{"a"})
	tree_1 := createTree(crc32_1)
	CreateCompactSSTable(data1, last1, 1, 1, filter_1, tree_1)

	time.Sleep(1 * time.Second)

	// Table 2
	data2 := SerializeEntryHelper("b", largeValue, false, false)
	last2 := SerializeEntryHelper("b", "", false, true)

	// Create Bloom filter and CRC32 tree for table 2
	crc32_2 := getHashes([][]byte{data2})
	filter_2 := createFilter([]string{"b"})
	tree_2 := createTree(crc32_2)
	CreateCompactSSTable(data2, last2, 1, 1, filter_2, tree_2)

	files := openAllDataFiles(t)
	folderPath := filepath.Join(dataPath, "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join(folderPath, "usertable-3-data.bin")
	MergeTables(files, newFilePath)
	cleanUpOldFiles(t, files)

	// Verify results
	resultA := SearchAll([]byte("a"), false)
	checkEqual(t, len(resultA), len(largeValue), "Length mismatch for key a")
	checkEqual(t, string(resultA), largeValue, "Incorrect value for key a")

	resultB := SearchAll([]byte("b"), false)
	checkEqual(t, len(resultB), len(largeValue), "Length mismatch for key b")
	checkEqual(t, string(resultB), largeValue, "Incorrect value for key b")

	// Additional verification that the merged file exists
	if _, err := os.Stat(newFilePath); os.IsNotExist(err) {
		t.Errorf("Expected merged file %s to exist, but it doesn't", newFilePath)
	}
}

func TestCompactionMultiBlockKeysCompact(t *testing.T) {
	dataPath := getDataPath()
	os.RemoveAll(dataPath)

	// Make a large key that spans more than one block
	largeKey := strings.Repeat("K", int(blockSize)*2)
	value := "short-val"

	// Table 1
	data1 := SerializeEntryHelper(largeKey, value, false, false)
	last1 := SerializeEntryHelper(largeKey, "", false, true)

	// Create Bloom filter and CRC32 tree for table 1
	crc32_1 := getHashes([][]byte{data1})
	filter_1 := createFilter([]string{largeKey})
	tree_1 := createTree(crc32_1)
	CreateCompactSSTable(data1, last1, 1, 1, filter_1, tree_1)

	time.Sleep(1 * time.Second)

	// Table 2 with a second large key
	largeKey2 := strings.Repeat("Q", int(blockSize)*2)
	data2 := SerializeEntryHelper(largeKey2, value, false, false)
	last2 := SerializeEntryHelper(largeKey2, "", false, true)

	// Create Bloom filter and CRC32 tree for table 2
	crc32_2 := getHashes([][]byte{data2})
	filter_2 := createFilter([]string{largeKey2})
	tree_2 := createTree(crc32_2)
	CreateCompactSSTable(data2, last2, 1, 1, filter_2, tree_2)

	// Merge tables
	files := openAllDataFiles(t)
	folderPath := filepath.Join(dataPath, "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join(folderPath, "usertable-3-compact.bin")
	MergeTables(files, newFilePath)
	cleanUpOldFiles(t, files)

	// Verify correct retrieval of both large keys
	result1 := SearchAll([]byte(largeKey), false)
	checkEqual(t, string(result1), value, "Incorrect value for large key 1")

	result2 := SearchAll([]byte(largeKey2), false)
	checkEqual(t, string(result2), value, "Incorrect value for large key 2")

	// Additional verification that the merged file exists
	if _, err := os.Stat(newFilePath); os.IsNotExist(err) {
		t.Errorf("Expected merged file %s to exist, but it doesn't", newFilePath)
	}

	os.RemoveAll(dataPath)
}

func TestCompactionMultiBlockKeysSeparate(t *testing.T) {
	dataPath := getDataPath()
	os.RemoveAll(dataPath)

	// Make a large key that spans more than one block
	largeKey := strings.Repeat("K", int(blockSize)*2)
	value := "short-val"

	// Table 1
	data1 := SerializeEntryHelper(largeKey, value, false, false)
	last1 := SerializeEntryHelper(largeKey, "", false, true)

	// Create metadata for table 1
	crc32_1 := getHashes([][]byte{data1})
	filter_1 := createFilter([]string{largeKey})
	tree_1 := createTree(crc32_1)

	// Create separate files (data, filter, index)
	CreateSeparatedSSTable(data1, last1, 1, 1, filter_1, tree_1)

	time.Sleep(1 * time.Second)

	// Table 2 with a second large key
	largeKey2 := strings.Repeat("Q", int(blockSize)*2)
	data2 := SerializeEntryHelper(largeKey2, value, false, false)
	last2 := SerializeEntryHelper(largeKey2, "", false, true)

	// Create metadata for table 2
	crc32_2 := getHashes([][]byte{data2})
	filter_2 := createFilter([]string{largeKey2})
	tree_2 := createTree(crc32_2)

	// Create separate files (data, filter, index)
	CreateSeparatedSSTable(data2, last2, 1, 1, filter_2, tree_2)

	// Merge tables
	files := openAllDataFiles(t)
	folderPath := filepath.Join(dataPath, "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join(folderPath, "usertable-3-data.bin")
	MergeTables(files, newFilePath)
	cleanUpOldFiles(t, files)

	// Verify correct retrieval of both large keys
	result1 := SearchAll([]byte(largeKey), false)
	checkEqual(t, string(result1), value, "Incorrect value for large key 1")

	result2 := SearchAll([]byte(largeKey2), false)
	checkEqual(t, string(result2), value, "Incorrect value for large key 2")

	os.RemoveAll(dataPath)
}

func TestCompactionLargeKeyLargeValueSeparate(t *testing.T) {
	dataPath := getDataPath()
	os.RemoveAll(dataPath)

	// Make a large key and value that span more than one block
	largeKey := strings.Repeat("K", int(blockSize)*2)
	value := strings.Repeat("X", int(blockSize)*2)

	// Table 1
	data1 := SerializeEntryHelper(largeKey, value, false, false)
	last1 := SerializeEntryHelper(largeKey, "", false, true)

	// Create metadata for table 1
	crc32_1 := getHashes([][]byte{data1})
	filter_1 := createFilter([]string{largeKey})
	tree_1 := createTree(crc32_1)

	// Create separate files (data, filter, index)
	CreateSeparatedSSTable(data1, last1, 1, 1, filter_1, tree_1)

	time.Sleep(1 * time.Second)

	// Table 2 with a second large key
	largeKey2 := strings.Repeat("Q", int(blockSize)*2)
	data2 := SerializeEntryHelper(largeKey2, value, false, false)
	last2 := SerializeEntryHelper(largeKey2, "", false, true)

	// Create metadata for table 2
	crc32_2 := getHashes([][]byte{data2})
	filter_2 := createFilter([]string{largeKey2})
	tree_2 := createTree(crc32_2)

	// Create separate files (data, filter, index)
	CreateSeparatedSSTable(data2, last2, 1, 1, filter_2, tree_2)

	// Merge tables
	files := openAllDataFiles(t)
	folderPath := filepath.Join(dataPath, "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join(folderPath, "usertable-3-data.bin")
	MergeTables(files, newFilePath)
	cleanUpOldFiles(t, files)

	// Verify correct retrieval of both large keys
	result1 := SearchAll([]byte(largeKey), false)
	checkEqual(t, string(result1), value, "Incorrect value for large key 1")

	result2 := SearchAll([]byte(largeKey2), false)
	checkEqual(t, string(result2), value, "Incorrect value for large key 2")

	os.RemoveAll(dataPath)
}

func TestCompactionLargeKeyLargeValueCompact(t *testing.T) {
	dataPath := getDataPath()
	os.RemoveAll(dataPath)

	// Napravi ključ i vrednost koji prelaze blok
	largeKey := strings.Repeat("K", int(blockSize)*2)
	value := strings.Repeat("X", int(blockSize)*2)

	// Tabela 1
	data1 := SerializeEntryHelper(largeKey, value, false, false)
	last1 := SerializeEntryHelper(largeKey, "", false, true)

	crc32_1 := getHashes([][]byte{data1})
	filter_1 := createFilter([]string{largeKey})
	tree_1 := createTree(crc32_1)

	CreateCompactSSTable(data1, last1, 1, 1, filter_1, tree_1)

	time.Sleep(1 * time.Second)

	// Tabela 2 sa drugim velikim ključem
	largeKey2 := strings.Repeat("Q", int(blockSize)*2)
	data2 := SerializeEntryHelper(largeKey2, value, false, false)
	last2 := SerializeEntryHelper(largeKey2, "", false, true)

	crc32_2 := getHashes([][]byte{data2})
	filter_2 := createFilter([]string{largeKey2})
	tree_2 := createTree(crc32_2)

	CreateCompactSSTable(data2, last2, 1, 1, filter_2, tree_2)

	// Merge-uj tabele
	files := openAllDataFiles(t)
	folderPath := filepath.Join(dataPath, "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join(folderPath, "usertable-3-compact.bin")
	MergeTables(files, newFilePath)
	cleanUpOldFiles(t, files)

	// Proveri ispravnost pretrage
	result1 := SearchAll([]byte(largeKey), false)
	checkEqual(t, string(result1), value, "Incorrect value for large key 1")

	result2 := SearchAll([]byte(largeKey2), false)
	checkEqual(t, string(result2), value, "Incorrect value for large key 2")

	os.RemoveAll(dataPath)
}

func TestCompactionMixedSizesCompact(t *testing.T) {
	dataPath := getDataPath()
	os.RemoveAll(dataPath)

	// Large entries
	largeKey1 := strings.Repeat("K", int(blockSize)*2)
	largeKey2 := strings.Repeat("Q", int(blockSize)*2)
	largeValue := strings.Repeat("X", int(blockSize)*2)

	// Small entries
	smallKey1 := "apple"
	smallKey2 := "banana"
	smallValue := "fruit"

	// --- Table 1: One large and one small entry ---
	entry1_1 := SerializeEntryHelper(largeKey1, largeValue, false, false)
	entry1_2 := SerializeEntryHelper(smallKey1, smallValue, false, false)
	data1 := append(entry1_1, entry1_2...)
	last1 := SerializeEntryHelper(smallKey1, "", false, true)

	crc1 := getHashes([][]byte{entry1_1, entry1_2})
	filter1 := createFilter([]string{largeKey1, smallKey1})
	tree1 := createTree(crc1)

	CreateSeparatedSSTable(data1, last1, 1, 1, filter1, tree1)

	time.Sleep(1 * time.Second)

	// --- Table 2: Another large and another small entry ---
	entry2_1 := SerializeEntryHelper(largeKey2, largeValue, false, false)
	entry2_2 := SerializeEntryHelper(smallKey2, smallValue, false, false)
	data2 := append(entry2_1, entry2_2...)
	last2 := SerializeEntryHelper(smallKey2, "", false, true)

	crc2 := getHashes([][]byte{entry2_1, entry2_2})
	filter2 := createFilter([]string{largeKey2, smallKey2})
	tree2 := createTree(crc2)

	CreateCompactSSTable(data2, last2, 1, 1, filter2, tree2)

	// --- Perform compaction ---
	files := openAllDataFiles(t)
	folderPath := filepath.Join(dataPath, "L1")
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

	os.RemoveAll(dataPath)
}

func TestCompactionMixedSizesSeparate(t *testing.T) {
	dataPath := getDataPath()
	os.RemoveAll(dataPath)

	// Large entries
	largeKey1 := strings.Repeat("K", int(blockSize)*2)
	largeKey2 := strings.Repeat("Q", int(blockSize)*2)
	largeValue := strings.Repeat("X", int(blockSize)*2)

	// Small entries
	smallKey1 := "apple"
	smallKey2 := "banana"
	smallValue := "fruit"

	// --- Table 1: One large and one small entry ---
	entry1_1 := SerializeEntryHelper(largeKey1, largeValue, false, false)
	entry1_2 := SerializeEntryHelper(smallKey1, smallValue, false, false)
	data1 := append(entry1_1, entry1_2...)
	last1 := SerializeEntryHelper(smallKey1, "", false, true)

	crc1 := getHashes([][]byte{entry1_1, entry1_2})
	filter1 := createFilter([]string{largeKey1, smallKey1})
	tree1 := createTree(crc1)

	CreateSeparatedSSTable(data1, last1, 1, 1, filter1, tree1)

	time.Sleep(1 * time.Second)

	// --- Table 2: Another large and another small entry ---
	entry2_1 := SerializeEntryHelper(largeKey2, largeValue, false, false)
	entry2_2 := SerializeEntryHelper(smallKey2, smallValue, false, false)
	data2 := append(entry2_1, entry2_2...)
	last2 := SerializeEntryHelper(smallKey2, "", false, true)

	crc2 := getHashes([][]byte{entry2_1, entry2_2})
	filter2 := createFilter([]string{largeKey2, smallKey2})
	tree2 := createTree(crc2)

	CreateSeparatedSSTable(data2, last2, 1, 1, filter2, tree2)

	// --- Perform compaction ---
	files := openAllDataFiles(t)
	folderPath := filepath.Join(dataPath, "L1")
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

	os.RemoveAll(dataPath)
}

func TestCompactionWithManyOverlappingKeys(t *testing.T) {
	dataPath := getDataPath()
	os.RemoveAll(dataPath)

	// Table 1: keys a to e
	data1 := []byte{}
	entry1a := SerializeEntryHelper("a", "1", false, false)
	entry1b := SerializeEntryHelper("b", "2", false, false)
	entry1c := SerializeEntryHelper("c", "3", false, false)
	entry1d := SerializeEntryHelper("d", "4", false, false)
	entry1e := SerializeEntryHelper("e", "5", false, false)
	data1 = append(data1, entry1a...)
	data1 = append(data1, entry1b...)
	data1 = append(data1, entry1c...)
	data1 = append(data1, entry1d...)
	data1 = append(data1, entry1e...)
	last1 := SerializeEntryHelper("e", "", false, true)

	crc1 := getHashes([][]byte{entry1a, entry1b, entry1c, entry1d, entry1e})
	filter1 := createFilter([]string{"a", "b", "c", "d", "e"})
	tree1 := createTree(crc1)

	CreateCompactSSTable(data1, last1, 5, 2, filter1, tree1)
	time.Sleep(1 * time.Second)

	// Table 2: overlapping keys b to g
	data2 := []byte{}
	entry2b := SerializeEntryHelper("b", "20", false, false)
	entry2c := SerializeEntryHelper("c", "30", false, false)
	entry2f := SerializeEntryHelper("f", "6", false, false)
	entry2g := SerializeEntryHelper("g", "7", false, false)
	data2 = append(data2, entry2b...)
	data2 = append(data2, entry2c...)
	data2 = append(data2, entry2f...)
	data2 = append(data2, entry2g...)
	last2 := SerializeEntryHelper("g", "", false, true)

	crc2 := getHashes([][]byte{entry2b, entry2c, entry2f, entry2g})
	filter2 := createFilter([]string{"b", "c", "f", "g"})
	tree2 := createTree(crc2)

	CreateCompactSSTable(data2, last2, 1, 1, filter2, tree2)
	time.Sleep(1 * time.Second)

	// Table 3: overlapping keys a, d, h
	data3 := []byte{}
	entry3a := SerializeEntryHelper("a", "100", false, false)
	entry3d := SerializeEntryHelper("d", "400", false, false)
	entry3h := SerializeEntryHelper("h", "8", false, false)
	data3 = append(data3, entry3a...)
	data3 = append(data3, entry3d...)
	data3 = append(data3, entry3h...)
	last3 := SerializeEntryHelper("h", "", false, true)

	crc3 := getHashes([][]byte{entry3a, entry3d, entry3h})
	filter3 := createFilter([]string{"a", "d", "h"})
	tree3 := createTree(crc3)

	CreateCompactSSTable(data3, last3, 1, 1, filter3, tree3)
	time.Sleep(1 * time.Second)

	// Merge tables
	files := openAllDataFiles(t)
	folderPath := filepath.Join(dataPath, "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join(dataPath, "L1", "usertable-3-compact.bin")
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

	os.RemoveAll(dataPath)
}

func TestCompactionNewerKeysOverrideOlder(t *testing.T) {
	dataPath := getDataPath()
	os.RemoveAll(dataPath)

	// Table 1: older values
	data1 := []byte{}
	entry1_1 := SerializeEntryHelper("key1", "old_value1", false, false)
	entry1_2 := SerializeEntryHelper("key2", "old_value2", false, false)
	data1 = append(data1, entry1_1...)
	data1 = append(data1, entry1_2...)
	last1 := SerializeEntryHelper("key2", "", false, true)

	crc1 := getHashes([][]byte{entry1_1, entry1_2})
	filter1 := createFilter([]string{"key1", "key2"})
	tree1 := createTree(crc1)

	CreateCompactSSTable(data1, last1, 2, 2, filter1, tree1)

	// Wait a bit so next SSTable has newer timestamps
	time.Sleep(1 * time.Second)

	// Table 2: newer values overwrite some keys
	data2 := []byte{}
	entry2_1 := SerializeEntryHelper("key1", "new_value1", false, false)
	entry2_2 := SerializeEntryHelper("key3", "new_value3", false, false)
	data2 = append(data2, entry2_1...)
	data2 = append(data2, entry2_2...)
	last2 := SerializeEntryHelper("key3", "", false, true)

	crc2 := getHashes([][]byte{entry2_1, entry2_2})
	filter2 := createFilter([]string{"key1", "key3"})
	tree2 := createTree(crc2)

	CreateSeparatedSSTable(data2, last2, 2, 2, filter2, tree2)

	files := openAllDataFiles(t)
	folderPath := filepath.Join(dataPath, "L1")
	os.Mkdir(folderPath, 0755)
	newFilePath := filepath.Join(dataPath, "L1", "usertable-3-compact.bin")
	MergeTables(files, newFilePath)

	time.Sleep(1 * time.Second)
	CreateSeparatedSSTable(data1, last1, 2, 2, filter1, tree1)

	// Verify that after compaction, Find returns newest values
	checkEqual(t, string(SearchAll([]byte("key1"), false)), "old_value1", "Expected key1 to have older value")
	checkEqual(t, string(SearchAll([]byte("key2"), false)), "old_value2", "Expected key2 to keep old value (no overwrite)")
	checkEqual(t, string(SearchAll([]byte("key3"), false)), "new_value3", "Expected key3 to have new value")
	cleanUpOldFiles(t, files)
}


