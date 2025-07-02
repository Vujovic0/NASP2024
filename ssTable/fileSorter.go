package ssTable

import (
	"cmp"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
)

type FileGen struct {
	filePath string
	gen      int
}

// Takes sstable names
// Sorts tables of a single LSM tree level in rising order
func sortTableNames(names []string) []string {
	genCmp := func(a, b FileGen) int {
		return cmp.Compare(b.gen, a.gen)
	}

	sortedPaths := make([]FileGen, len(names))

	for i, filePath := range names {
		splitFile := strings.Split(filePath, "-")
		if len(splitFile) < 2 {
			panic("invalid SSTable file name format: " + filePath)
		}
		genString := splitFile[1]
		gen, err := strconv.Atoi(genString)
		if err != nil {
			panic("can not turn characters to integer")
		}
		sortedPaths[i] = FileGen{filePath: filePath, gen: gen}
	}

	slices.SortFunc(sortedPaths, genCmp)
	strPaths := make([]string, len(sortedPaths))

	for i, fileGen := range sortedPaths {
		strPaths[i] = fileGen.filePath
	}
	return strPaths
}

// Takes filePath to data folder where LSM tree is present
// Returns names of all LSM tree levels
func getLevelNames(filePath string) []string {
	dirs, err := os.ReadDir(filePath)
	if err != nil {
		panic(err)
	}

	levelsString := getLevelsStrings(dirs)
	levelsInt := getLevelsInt(levelsString)
	slices.Sort(levelsInt)

	levelNames := make([]string, len(levelsInt))
	for i, level := range levelsInt {
		levelStr := strconv.FormatInt(int64(level), 10)
		dirName := strings.Join([]string{"L", levelStr}, "")
		levelNames[i] = dirName
	}

	return levelNames
}

// Takes dir entries of data directory where an entry is a folder representing a level
// Returns numbers of all levels as strings trimming the "L"
func getLevelsStrings(dirs []os.DirEntry) []string {
	levels := make([]string, 0)

	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}

		dirInfo, err := dir.Info()
		if err != nil {
			panic(err)
		}

		// all levels start with L, example L10 is level 10
		levels = append(levels, dirInfo.Name()[1:])
	}
	return levels
}

// Converts all string levels to int
func getLevelsInt(levelsString []string) []int {
	levels := make([]int, len(levelsString))

	for i, levelString := range levelsString {
		level, err := strconv.Atoi(levelString)

		if err != nil {
			panic(err)
		}

		levels[i] = level
	}

	return levels
}

/*
Takes relative path from ssTable package to level example data/L3.

Returns all names of sstables in that directory
*/
func getTableNames(levelPath string) []string {
	files, err := os.ReadDir(levelPath)

	if err != nil {
		panic(err)
	}

	names := make([]string, 0)
	for _, file := range files {
		fileInfo, err := file.Info()
		if err != nil {
			panic(err)
		}

		fileName := fileInfo.Name()
		if strings.HasSuffix(fileName, "compact.bin") || strings.HasSuffix(fileName, "data.bin") {
			names = append(names, fileName)
		}
	}

	return names
}

/*
Path relative to caller's package
*/
func GetReadOrder(dataPath string) []string {
	levels := getLevelNames(dataPath)
	levelPaths := getLevelPaths(dataPath, levels)
	orderedPaths := make([]string, 0)
	for _, levelPath := range levelPaths {
		tableNames := getTableNames(levelPath)
		tableNames = sortTableNames(tableNames)
		tablePaths := getTablePaths(levelPath, tableNames)
		orderedPaths = append(orderedPaths, tablePaths...)
	}

	return orderedPaths
}

func getLevelPaths(dataPath string, sortedLevels []string) []string {
	sortedPaths := make([]string, len(sortedLevels))

	for i, levelStr := range sortedLevels {
		levelPath := filepath.Join(dataPath, levelStr)
		sortedPaths[i] = levelPath
	}

	return sortedPaths
}

func getTablePaths(levelPath string, tableNames []string) []string {
	tablePaths := make([]string, len(tableNames))

	for i, tableName := range tableNames {
		tablePath := filepath.Join(levelPath, tableName)
		tablePaths[i] = tablePath
	}

	return tablePaths
}

// returns absolute path to the /data folder regardless of where you run from
func getDataPath() string {
	_, currentFile, _, _ := runtime.Caller(0)
	baseDir := filepath.Dir(currentFile) // .../ssTable
	return filepath.Join(baseDir, "data")
}
