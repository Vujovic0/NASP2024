//go:build ignore
// +build ignore

package ssTable

import (
	"os"
	"strings"
	"testing"
)

func TestCompactionBasicMerge(t *testing.T) {
	os.RemoveAll("data")

	// Table 1
	data1 := SerializeKeyValue("a", "1", false, false)
	data1 = append(data1, SerializeKeyValue("b", "2", false, false)...)
	last1 := SerializeKeyValue("b", "", false, true)
	CreateSeparatedSSTable(data1, last1, 2, 2)

	// Table 2
	data2 := SerializeKeyValue("c", "3", false, false)
	last2 := SerializeKeyValue("c", "", false, true)
	CreateSeparatedSSTable(data2, last2, 1, 1)

	files := openAllDataFiles(t)
	MergeTables(files, "data/L1.userTable-3.compact.bin")
	cleanUpOldFiles(t, files)

	result := Find("a")
	checkEqual(t, string(result), "1", "Incorrect value after compaction")

	result = Find("b")
	checkEqual(t, string(result), "2", "Incorrect value after compaction")

	result = Find("c")
	checkEqual(t, string(result), "3", "Incorrect value after compaction")
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

func deleteAllDataFiles(t *testing.T, files []*os.File) {
	t.Helper()
	for _, file := range files {
		os.Remove(file.Name())
	}
}

func cleanUpOldFiles(t *testing.T, files []*os.File) {
	deleteAllDataFiles(t, files)
	closeAllDataFiles(t, files)
}
