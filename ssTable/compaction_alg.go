package ssTable

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Vujovic0/NASP2024/config"
)

// LeveledCompaction performs leveled compaction for a given level.
// Assumes the files in the target level directory are sorted from oldest to newest.
func LeveledCompaction(level int) {
	nextLevel := level + 1
	inputDir := filepath.Join(getDataPath(), fmt.Sprintf("L%d", level))
	outputDir := filepath.Join(getDataPath(), fmt.Sprintf("L%d", nextLevel))
	_ = os.MkdirAll(outputDir, 0755)

	files, err := os.ReadDir(inputDir)
	if err != nil {
		panic("failed to read level directory: " + err.Error())
	}

	var dataFiles []string
	for _, file := range files {
		if strings.HasSuffix(file.Name(), "-data.bin") {
			dataFiles = append(dataFiles, filepath.Join(inputDir, file.Name()))
		}
	}

	// Sort files numerically by their generation number
	sort.Slice(dataFiles, func(i, j int) bool {
		return extractGeneration(dataFiles[i]) < extractGeneration(dataFiles[j])
	})

	// Open files
	var filePointers []*os.File
	for _, f := range dataFiles {
		fp, err := os.Open(f)
		if err != nil {
			panic("failed to open data file: " + err.Error())
		}
		filePointers = append(filePointers, fp)
	}

	// Merge all files into one compacted file in next level
	outputFileName := filepath.Join(outputDir, fmt.Sprintf("usertable-%d-compact.bin", GetGeneration(false)))
	MergeTables(filePointers, outputFileName)

	// Close file pointers
	for _, f := range filePointers {
		_ = f.Close()
	}

	fmt.Println("Leveled compaction complete: L", level, "-> L", nextLevel)
}

func extractGeneration(filePath string) int {
	base := filepath.Base(filePath)
	parts := strings.Split(base, "-")
	if len(parts) < 2 {
		return 0
	}
	var gen int
	fmt.Sscanf(parts[1], "%d", &gen)
	return gen
}

// SizeTieredCompaction performs compaction on files of similar size within a level (default: L0).
// Compacts only when there are at least `threshold` files of similar size (+/- margin).
func SizeTieredCompaction(level int) {

	inputDir := filepath.Join(getDataPath(), fmt.Sprintf("L%d", level))
	outputDir := filepath.Join(getDataPath(), fmt.Sprintf("L%d", level+1))
	_ = os.MkdirAll(outputDir, 0755)

	files, err := os.ReadDir(inputDir)
	if err != nil {
		panic("Failed to read level directory: " + err.Error())
	}

	var filePointers []*os.File
	var fileNames []string

	for _, entry := range files {
		name := entry.Name()
		if strings.HasSuffix(name, "-data.bin") || strings.HasSuffix(name, "-compact.bin") {
			fullPath := filepath.Join(inputDir, name)
			fp, err := os.Open(fullPath)
			if err != nil {
				continue
			}
			defer fp.Close()
			filePointers = append(filePointers, fp)
			fileNames = append(fileNames, fullPath)
		}
	}

	if len(filePointers) < config.TablesCount {
		fmt.Printf("Not enough files to compact in L%d. Found %d, need at least %d.\n", level, len(filePointers), config.TablesCount)
		return
	}

	writeToSameLevel := (level+1 >= config.MaxLevels)
	targetLevel := level
	if !writeToSameLevel {
		targetLevel = level + 1
	}

	targetDir := filepath.Join(getDataPath(), fmt.Sprintf("L%d", targetLevel))
	_ = os.MkdirAll(targetDir, 0755)

	var outputFile string
	if config.SeparateFiles {
		outputFile = filepath.Join(targetDir, fmt.Sprintf("usertable-%d-data.bin", GetGeneration(true)))
	} else {
		outputFile = filepath.Join(targetDir, fmt.Sprintf("usertable-%d-compact.bin", GetGeneration(true)))
	}

	MergeTables(filePointers, outputFile)

	for i, f := range filePointers {
		f.Close()
		err = os.Remove(fileNames[i])
		if err != nil {
			print(err)
		}
		if strings.HasSuffix(fileNames[i], "data.bin") {
			filePrefix := strings.TrimSuffix(fileNames[i], "data.bin")
			_ = os.Remove(filePrefix + "tree.bin")
			_ = os.Remove(filePrefix + "index.bin")
			_ = os.Remove(filePrefix + "summary.bin")
			_ = os.Remove(filePrefix + "filter.bin")
		}
	}

	fmt.Printf("Size-tiered compaction complete: L%d ➝ L%d (%d files ➝ 1)\n", level, targetLevel, len(filePointers))

	if !writeToSameLevel {
		SizeTieredCompaction(targetLevel)
	}
}
