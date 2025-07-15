package ssTable

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Vujovic0/NASP2024/config"
)

func TestSizeTieredCompaction_AllFilesMergedAndCleaned(t *testing.T) {
	// Setup: clean and reset config
	os.RemoveAll(getDataPath())
	config.VariableEncoding = false

	// Create multiple SSTables in L0 (separated structure)
	data1 := []byte{}
	data1 = append(data1, SerializeEntryHelper("a", "1", false, false)...)
	data1 = append(data1, SerializeEntryHelper("b", "2", false, false)...)
	last1 := SerializeEntryHelper("b", "", false, true)
	CreateSeparatedSSTable(data1, last1, 2, 2)
	time.Sleep(1 * time.Second)

	data2 := []byte{}
	data2 = append(data2, SerializeEntryHelper("a", "100", false, false)...)
	data2 = append(data2, SerializeEntryHelper("c", "3", false, false)...)
	last2 := SerializeEntryHelper("c", "", false, true)
	CreateSeparatedSSTable(data2, last2, 2, 2)
	time.Sleep(1 * time.Second)

	// Perform compaction
	SizeTieredCompaction(0)

	// Verify only one -data.bin file remains
	files, err := os.ReadDir(filepath.Join(getDataPath(), "L0"))
	if err != nil {
		t.Fatal("Failed to read directory:", err)
	}
	count := 0
	for _, f := range files {
		if strings.HasSuffix(f.Name(), "-data.bin") {
			count++
		}
	}
	if count != 1 {
		t.Errorf("Expected only 1 data file after compaction, got %d", count)
	}

	// Validate content: latest value wins
	checkEqual(t, string(SearchAll([]byte("a"), false)), "100", "Expected latest value a=100")
	checkEqual(t, string(SearchAll([]byte("b"), false)), "2", "Expected b=2")
	checkEqual(t, string(SearchAll([]byte("c"), false)), "3", "Expected c=3")
}
