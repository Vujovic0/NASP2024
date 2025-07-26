package ssTable

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func constructTree(file *os.File) *MerkleTree {
	if file == nil {
		return nil
	}
	crc32Values := getCrc32Values(file)
	return NewMerkleTreeFromHashes(crc32Values)
}

// Returns filepath of table by generation
func getTableNameByGeneration(generation int) string {
	tablePaths := GetReadOrder(getDataPath())
	maxGeneration := GetGeneration(false)
	if maxGeneration < uint64(generation) {
		return ""
	}
	for _, tablePath := range tablePaths {
		splitTablePath := strings.Split(tablePath, "-")
		if len(splitTablePath) < 2 {
			return ""
		}
		tableGeneration, err := strconv.Atoi(splitTablePath[1])

		if err != nil {
			panic(fmt.Sprintf("can not convert %s to int", splitTablePath[1]))
		}

		if generation == tableGeneration {
			return tablePath
		}
	}
	return ""
}

func getCrc32Values(file *os.File) []uint32 {
	limit := GetLimits([]*os.File{file})[0]
	blockOffset := uint64(0)
	entries := make([]*Entry, 0)
	var err error
	crc32Values := make([]uint32, 0)
	for {
		entries, blockOffset, err = GetBlockEntries(file, blockOffset, limit)
		if err != nil {
			panic(err)
		}
		if len(entries) == 0 {
			break
		}

		for _, entry := range entries {
			crc32Values = append(crc32Values, entry.crc)
		}
	}

	return crc32Values
}

// Takes generation of wanted table, returns path to table if found and true if validation was successful
func ValidateSSTable(generation int) (string, bool) {
	tablePath := getTableNameByGeneration(generation)
	if tablePath == "" {
		return "", false
	}
	file := getFilePointer(tablePath)
	defer file.Close()
	constructedMerkleTree := constructTree(file)

	if constructedMerkleTree == nil {
		fmt.Println("Tabela sa ovom generacijom ne postoji...")
		return "", false
	}

	deserializedMerkleTree, _ := fetchMerkleTree(file)
	differences := FindAllLeafMerkleDifferences(deserializedMerkleTree, constructedMerkleTree)
	if len(differences) == 0 {
		return tablePath, true
	}
	printBadHashes(differences)
	return tablePath, false
}

func getFilePointer(tablePath string) *os.File {
	if len(tablePath) == 0 {
		return nil
	}
	file, err := os.Open(tablePath)
	if err != nil {
		panic(err)
	}
	return file
}

func printBadHashes(differences []MerkleDiff) {
	fmt.Println("Errors were found during validation:")
	for _, difference := range differences {
		fmt.Printf("expected: %d got: %d", difference.HashOld, difference.HashNew)
	}
}
