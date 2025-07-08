package ssTable

import (
	"bytes"
	"encoding/binary"
	"os"
	"strconv"

	"github.com/Vujovic0/NASP2024/blockManager"
	"github.com/Vujovic0/NASP2024/config"
)

// Finalne verzije svih FIND funkcija potrebne za SSTable funkcionalnosti

// Main FIND funkcija koja traži ključ kroz sve levele i sve formate (compact i separated)
func Find(key []byte) []byte {
	generation := getGeneration(false)
	levels, err := os.ReadDir("data")
	if err != nil {
		panic(err)
	}
	files := make(map[string]string)
	for _, level := range levels {
		if level.Name() == "metaData.bin" {
			continue
		}
		filesToAppend, err := os.ReadDir("data" + string(os.PathSeparator) + level.Name())
		if err != nil {
			panic(err)
		}
		for _, file := range filesToAppend {
			files[file.Name()] = "data" + string(os.PathSeparator) + level.Name() + string(os.PathSeparator) + file.Name()
		}
	}

	var offset uint64
	var valueBytes []byte

	for iterator := 1; iterator <= int(generation); iterator++ {
		genStr := strconv.FormatUint(uint64(iterator), 10)
		fileName := "usertable-" + genStr + "-compact.bin"
		if _, ok := files[fileName]; ok {
			valueBytes, err = findSeparated(files[fileName], key, 0)
			if err != nil {
				panic(err)
			}
			if valueBytes != nil {
				return valueBytes
			}
			continue
		}

		fileName = "usertable-" + genStr + "-summary.bin"
		if path, ok := files[fileName]; ok {
			valueBytes, err = findSeparated(path, key, 0)
			if err != nil {
				panic(err)
			}
			if valueBytes == nil {
				continue
			}
			fileName = "usertable-" + genStr + "-index.bin"
			if !config.VariableEncoding {
				offset = binary.LittleEndian.Uint64(valueBytes)
			} else {
				offset, _ = binary.Uvarint(valueBytes)
			}
			valueBytes, err = findSeparated(files[fileName], key, offset)
			if err != nil {
				panic(err)
			}

			fileName = "usertable-" + genStr + "-data.bin"
			if !config.VariableEncoding {
				offset = binary.LittleEndian.Uint64(valueBytes)
			} else {
				offset, _ = binary.Uvarint(valueBytes)
			}
			valueBytes, err = findSeparated(files[fileName], key, offset)
			if err != nil {
				panic(err)
			}
			if valueBytes != nil {
				return valueBytes
			}
		}
	}
	return nil
}

// findSeparated se koristi za summary, index i data fajlove sa varijabilnim ili fiksnim enkodiranjem
func findSeparated(filePath string, key []byte, offset uint64) ([]byte, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var (
		dataBlockCheck bool
		tombstone      bool
		keys           [][]byte
		keyBytes       []byte
		valueBytes     []byte
		values         [][]byte
		lastValue      []byte
		keySizeLeft    uint64
		valueSizeLeft  uint64
		boundIndex     uint64
	)

	fileType := filePath[len(filePath)-11:]
	if fileType == "summary.bin" {
		maximumBound, err := getMaximumBound(file)
		if err != nil {
			panic(err)
		}
		if bytes.Compare(maximumBound, key) < 0 {
			return nil, nil
		}
		boundIndex = getBoundIndex(file)
	}
	blockOffset := offset

	for {
		if fileType[len(fileType)-8:] == "data.bin" {
			dataBlockCheck = true
		} else {
			dataBlockCheck = false
		}
		if boundIndex != 0 && boundIndex == blockOffset {
			return lastValue, nil
		}
		block := blockManager.ReadBlock(file, blockOffset)
		if block == nil {
			break
		}
		switch block.GetType() {
		case 0:
			keys, values, err = GetKeysType0(block, dataBlockCheck, boundIndex)
			if err != nil {
				panic(err)
			}
			index := FindLastSmallerKey(key, keys, dataBlockCheck, false)
			if index == -2 {
				if dataBlockCheck || len(lastValue) == 0 {
					return nil, nil
				}
				return lastValue, nil
			}
			if index != -1 {
				return values[index], nil
			}
			lastValue = values[len(values)-1]
		case 1:
			keyBytes, valueBytes, keySizeLeft, valueSizeLeft, err = GetKeysType1(block, dataBlockCheck, boundIndex)
			if err != nil {
				panic(err)
			}
			tombstone = dataBlockCheck && len(valueBytes) == 0 && valueSizeLeft == 0
		case 2:
			var k, v []byte
			k, v, keySizeLeft, valueSizeLeft, err = GetKeysType2(block, keySizeLeft, valueSizeLeft, tombstone, dataBlockCheck)
			if err != nil {
				panic(err)
			}
			keyBytes = append(keyBytes, k...)
			valueBytes = append(valueBytes, v...)
		case 3:
			var k, v []byte
			k, v, err = GetKeysType3(block, keySizeLeft, valueSizeLeft, tombstone, dataBlockCheck)
			if err != nil {
				panic(err)
			}
			keyBytes = append(keyBytes, k...)
			valueBytes = append(valueBytes, v...)
			cmp := bytes.Compare(keyBytes, key)
			if cmp == 0 {
				return valueBytes, nil
			} else if cmp > 0 {
				if dataBlockCheck || len(lastValue) == 0 {
					return nil, nil
				}
				return lastValue, nil
			}
			lastValue = valueBytes
			keyBytes = keyBytes[:0]
			valueBytes = valueBytes[:0]
			keySizeLeft, valueSizeLeft = 0, 0
			tombstone = false
		}
		blockOffset++
	}
	return lastValue, nil
}
