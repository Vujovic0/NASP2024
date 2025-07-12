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
	MergeTables(filePointers, outputFileName, globalDict, config.UseCompression)

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
func SizeTieredCompaction(level int, threshold int, marginBytes int64) {
	inputDir := filepath.Join(getDataPath(), fmt.Sprintf("L%d", level))
	_ = os.MkdirAll(inputDir, 0755)

	files, err := os.ReadDir(inputDir)
	if err != nil {
		panic("failed to read level directory: " + err.Error())
	}

	// Filter only data files
	var fileInfos []os.FileInfo
	for _, entry := range files {
		if strings.HasSuffix(entry.Name(), "-data.bin") || strings.HasSuffix(entry.Name(), "-compact.bin") {
			fp := filepath.Join(inputDir, entry.Name())
			info, err := os.Stat(fp)
			if err != nil {
				continue
			}
			fileInfos = append(fileInfos, info)
		}
	}

	// Group files by similar size
	groups := groupFilesBySize(fileInfos, marginBytes)
	for _, group := range groups {
		if len(group) < threshold {
			continue // not enough similar files to compact
		}

		// Sort files in group by generation (oldest to newest)
		sort.Slice(group, func(i, j int) bool {
			return extractGeneration(group[i].Name()) < extractGeneration(group[j].Name())
		})

		// Open files
		var filePointers []*os.File
		for _, fi := range group {
			fp, err := os.Open(filepath.Join(inputDir, fi.Name()))
			if err != nil {
				continue
			}
			filePointers = append(filePointers, fp)
		}

		// Create new compacted file in the same level (L0)
		outputFile := filepath.Join(inputDir, fmt.Sprintf("usertable-%d-compact.bin", GetGeneration(false)))
		MergeTables(filePointers, outputFile, globalDict, config.UseCompression)

		// Cleanup
		for _, fp := range filePointers {
			fp.Close()
			os.Remove(fp.Name()) // remove old compacted files
		}
		fmt.Println("STC complete: merged", len(group), "files.")
	}
}

func groupFilesBySize(fileInfos []os.FileInfo, margin int64) [][]os.FileInfo {
	var groups [][]os.FileInfo
	used := make([]bool, len(fileInfos))

	for i := range fileInfos {
		if used[i] {
			continue
		}
		var group []os.FileInfo
		sizeI := fileInfos[i].Size()
		group = append(group, fileInfos[i])
		used[i] = true

		for j := i + 1; j < len(fileInfos); j++ {
			if used[j] {
				continue
			}
			sizeJ := fileInfos[j].Size()
			if abs64(sizeI-sizeJ) <= margin {
				group = append(group, fileInfos[j])
				used[j] = true
			}
		}
		groups = append(groups, group)
	}
	return groups
}

func abs64(a int64) int64 {
	if a < 0 {
		return -a
	}
	return a
}
