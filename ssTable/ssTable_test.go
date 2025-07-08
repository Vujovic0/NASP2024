package ssTable

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/Vujovic0/NASP2024/blockManager"
)

func TestFooterOffsets(t *testing.T) {
	sstFilename := "testdata/testfile.sst"

	// Napravi sample elemente
	elements := []Element{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
		{Key: "c", Value: []byte("3")},
	}

	// Kreiraj SSTable fajl (poziv tvoje flush funkcije)
	err := Flush(elements, sstFilename)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Otvori fajl
	file, err := os.Open(sstFilename)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer file.Close()

	stat, _ := file.Stat()
	size := stat.Size()

	footerSize := int64(24)
	footerBytes := make([]byte, footerSize)
	_, err = file.ReadAt(footerBytes, size-footerSize)
	if err != nil {
		t.Fatalf("ReadAt footer failed: %v", err)
	}

	indexOffset := binary.LittleEndian.Uint64(footerBytes[0:8])
	summaryOffset := binary.LittleEndian.Uint64(footerBytes[8:16])
	boundOffset := binary.LittleEndian.Uint64(footerBytes[16:24])

	if indexOffset == 0 || summaryOffset == 0 || boundOffset == 0 {
		t.Fatalf("Footer offsets are zero: %v %v %v", indexOffset, summaryOffset, boundOffset)
	}
}

func prepareMockCompactFile(t *testing.T, path string, key, value []byte) {
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	blk := blockManager.NewBlock(3)
	blk.Add(key)
	blk.Add(value)
	blockManager.WriteBlock(file, blk)
}

func prepareMockSeparatedFiles(t *testing.T, gen int, key, value []byte) {
	genStr := "usertable-" + strconv.Itoa(gen)
	base := "data/level0"
	os.MkdirAll(base, os.ModePerm)

	// Data file
	dataFile := filepath.Join(base, genStr+"-data.bin")
	f, _ := os.Create(dataFile)
	defer f.Close()
	blk := blockManager.NewBlock(3)
	blk.Add(key)
	blk.Add(value)
	blockManager.WriteBlock(f, blk)

	// Summary file
	summaryFile := filepath.Join(base, genStr+"-summary.bin")
	summary, _ := os.Create(summaryFile)
	defer summary.Close()
	summary.Write(key)
	summary.Seek(100, 0)
	binary.Write(summary, binary.LittleEndian, uint64(0)) // boundIndex

	// Index file
	indexFile := filepath.Join(base, genStr+"-index.bin")
	idx, _ := os.Create(indexFile)
	defer idx.Close()
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, 0)
	idx.Write(buf)
}

func cleanTestData() {
	os.RemoveAll("data")
}

func TestFindCompact(t *testing.T) {
	cleanTestData()
	defer cleanTestData()
	os.MkdirAll("data/level0", os.ModePerm)
	key := []byte("alpha")
	value := []byte("bravo")
	prepareMockCompactFile(t, "data/level0/usertable-1-compact.bin", key, value)

	result := Find(key)
	if result == nil || !bytes.Equal(result, value) {
		t.Errorf("Expected %s, got %s", value, result)
	}
}

func TestFindSeparated(t *testing.T) {
	cleanTestData()
	defer cleanTestData()
	key := []byte("kilo")
	value := []byte("lima")
	prepareMockSeparatedFiles(t, 1, key, value)

	result := Find(key)
	if result == nil || !bytes.Equal(result, value) {
		t.Errorf("Expected %s, got %s", value, result)
	}
}

func TestFindNotFound(t *testing.T) {
	cleanTestData()
	defer cleanTestData()
	key := []byte("nonexistent")
	result := Find(key)
	if result != nil {
		t.Errorf("Expected nil, got %s", result)
	}
}

func TestPromoteLevel(t *testing.T) {
	cleanTestData()
	defer cleanTestData()
	os.MkdirAll("data/level0", os.ModePerm)
	os.WriteFile("data/level0/test.bin", []byte("123"), 0644)

	PromoteLevel(0)

	if _, err := os.Stat("data/level1/test.bin"); os.IsNotExist(err) {
		t.Errorf("Expected file to be promoted")
	}
	if _, err := os.Stat("data/level0/test.bin"); err == nil {
		t.Errorf("Expected file to be moved from level0")
	}
}
