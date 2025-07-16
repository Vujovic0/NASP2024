package ssTable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/Vujovic0/NASP2024/blockManager"
	"github.com/Vujovic0/NASP2024/config"
	"github.com/Vujovic0/NASP2024/probabilisticDataStructures/bloomFilter"
)

func searchCompact(filePath string, key []byte, prefix bool) ([]byte, uint64, error) {
	if filePath[len(filePath)-11:] != "compact.bin" {
		panic("Error: findCompact only works on compact sstables")
	}

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		panic("Error: can't get file info")
	}

	footerBlock := blockManager.ReadBlock(file, uint64(math.Ceil(float64(fileInfo.Size())/float64(blockSize))-1))
	footerData := footerBlock.GetData()

	indexStart := binary.LittleEndian.Uint64(footerData[17:25])
	summaryStart := binary.LittleEndian.Uint64(footerData[25:33])
	filterStart := binary.LittleEndian.Uint64(footerData[9+4*8 : 9+5*8])

	filter, _ := fetchFilter(file, filterStart)
	if !bloomFilter.SearchData(filter, string(key)) {
		return nil, 0, nil
	}

	maximumBound, err := GetMaximumBound(file)
	if err != nil {
		panic(err)
	}
	boundIndex := getBoundIndex(file)

	if bytes.Compare(maximumBound, key) == -1 {
		return nil, 0, nil
	}

	var blockOffset uint64 = summaryStart
	var dataBlockCheck bool = false
	var keys [][]byte
	var values [][]byte
	var currentSection byte = 0

	var valueBytes []byte
	var keyBytes []byte
	var valueSizeLeft uint64
	var keySizeLeft uint64
	var tombstone bool
	var lastValue []byte = nil
	var lastEntryOffset uint64 = 0

	for {
		if blockOffset == boundIndex {
			if !config.VariableHeader {
				blockOffset = binary.LittleEndian.Uint64(valueBytes)
			} else {
				blockOffset, _ = binary.Uvarint(valueBytes)
			}
			currentSection = 1
		}
		if currentSection == 0 && blockOffset >= boundIndex {
			break
		} else if currentSection == 1 && blockOffset >= summaryStart {
			break
		} else if currentSection == 2 {
			dataBlockCheck = true
			if blockOffset >= indexStart {
				break
			}
		}

		entryBlockOffset := blockOffset
		block := blockManager.ReadBlock(file, blockOffset)
		if block == nil {
			break
		}

		switch block.GetType() {
		case 0:
			keys, values, err = GetKeysType0(block, dataBlockCheck, boundIndex)
			if err != nil {
				panic("Error reading block")
			}

			index := FindLastSmallerKey(key, keys, dataBlockCheck, prefix)
			if index == -1 {
				if dataBlockCheck {
					blockOffset++
					continue
				}
				currentSection++
				if !config.VariableHeader {
					blockOffset = binary.LittleEndian.Uint64(values[len(values)-1])
				} else {
					blockOffset, _ = binary.Uvarint(values[len(values)-1])
				}
				continue
			} else if index == -2 {
				if dataBlockCheck {
					return nil, 0, nil
				}
				if lastValue == nil {
					return nil, 0, nil
				}
				return lastValue, lastEntryOffset, nil
			} else if dataBlockCheck {
				return values[index], entryBlockOffset, nil
			}

			if !config.VariableHeader {
				blockOffset = binary.LittleEndian.Uint64(values[index])
			} else {
				blockOffset, _ = binary.Uvarint(values[index])
			}
			currentSection++
			lastValue = nil
			continue

		case 1:
			keyBytes, valueBytes, keySizeLeft, valueSizeLeft, err = GetKeysType1(block, dataBlockCheck, boundIndex)
			if err != nil {
				panic(err)
			}

			if dataBlockCheck {
				tombstone = len(valueBytes) == 0 && valueSizeLeft == 0
			}
			blockOffset++

		case 2:
			keyBytesToAppend, valueBytesToAppend, keySizeLeftNew, valueSizeLeftNew, err := GetKeysType2(block, keySizeLeft, valueSizeLeft, tombstone, dataBlockCheck)
			if err != nil {
				panic(err)
			}
			keyBytes = append(keyBytes, keyBytesToAppend...)
			valueBytes = append(valueBytes, valueBytesToAppend...)
			keySizeLeft = keySizeLeftNew
			valueSizeLeft = valueSizeLeftNew
			blockOffset++

		case 3:
			keyBytesToAppend, valueBytesToAppend, err := GetKeysType3(block, keySizeLeft, valueSizeLeft, tombstone, dataBlockCheck)
			if err != nil {
				panic(err)
			}
			keyBytes = append(keyBytes, keyBytesToAppend...)
			valueBytes = append(valueBytes, valueBytesToAppend...)

			var compareResult int
			if prefix {
				prefixFlag := bytes.HasPrefix(keyBytes, key)
				if prefixFlag {
					compareResult = 0
				} else {
					compareResult = bytes.Compare(keyBytes, key)
				}
			} else {
				compareResult = bytes.Compare(keyBytes, key)
			}

			if compareResult == 0 {
				if dataBlockCheck {
					return valueBytes, entryBlockOffset, nil
				}
				if !config.VariableHeader {
					blockOffset = binary.LittleEndian.Uint64(valueBytes)
				} else {
					blockOffset, _ = binary.Uvarint(valueBytes)
				}
				currentSection++

				lastValue = nil
				keyBytes = keyBytes[:0]
				valueBytes = valueBytes[:0]
				keySizeLeft = 0
				valueSizeLeft = 0
				tombstone = false
				continue

			} else if compareResult > 0 {
				if dataBlockCheck {
					return nil, 0, nil
				}
				if lastValue == nil {
					return nil, 0, nil
				}
				return lastValue, lastEntryOffset, nil
			} else if compareResult < 0 && !dataBlockCheck {
				if !config.VariableHeader {
					blockOffset = binary.LittleEndian.Uint64(valueBytes)
				} else {
					blockOffset, _ = binary.Uvarint(valueBytes)
				}
				currentSection++

				lastValue = nil
				keyBytes = keyBytes[:0]
				valueBytes = valueBytes[:0]
				keySizeLeft = 0
				valueSizeLeft = 0
				tombstone = false
				continue
			}

			lastValue = valueBytes
			lastEntryOffset = entryBlockOffset

			blockOffset++
			keyBytes = keyBytes[:0]
			valueBytes = valueBytes[:0]
			keySizeLeft = 0
			valueSizeLeft = 0
			tombstone = false
		}
	}

	return nil, 0, nil
}

// When matching key is found, returns its value
// If key stops being larger than comparing key, return value of last key
// Returns nil if not found
func searchSeparated(filePath string, key []byte, offset uint64, prefix bool) ([]byte, uint64, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()

	var dataBlockCheck bool = false
	var tombstone bool = false

	var keys [][]byte = make([][]byte, 0)
	var keyBytes []byte = make([]byte, 0)
	var valueBytes []byte = make([]byte, 0)
	var values [][]byte = make([][]byte, 0)
	var lastValue []byte = make([]byte, 0)

	var keySizeLeft uint64 = 0
	var valueSizeLeft uint64 = 0

	var boundIndex uint64 = 0
	var entryBlockOffset uint64 = 0
	var lastEntryOffset uint64 = 0

	fileType := filePath[len(filePath)-11:]
	if fileType == "summary.bin" {
		maximumBound, err := GetMaximumBound(file)
		if err != nil {
			panic(err)
		}
		if bytes.Compare(maximumBound, key) < 0 {
			return nil, 0, nil
		}
		boundIndex = getBoundIndex(file)
	}

	var blockOffset uint64 = offset

	for {
		if fileType[len(fileType)-8:] == "data.bin" {
			dataBlockCheck = true
		} else {
			dataBlockCheck = false
		}
		if boundIndex != 0 && boundIndex == blockOffset {
			return lastValue, lastEntryOffset, nil
		}

		block := blockManager.ReadBlock(file, blockOffset)
		if block == nil {
			break
		}
		entryBlockOffset = block.GetOffset() // always set current block offset

		switch block.GetType() {
		case 0:
			keys, values, err = GetKeysType0(block, dataBlockCheck, boundIndex)
			if err != nil {
				panic(err)
			}
			index := FindLastSmallerKey(key, keys, dataBlockCheck, prefix)
			switch index {
			case -2:
				if dataBlockCheck {
					return nil, 0, nil
				}
				if len(lastValue) == 0 {
					return nil, 0, nil
				}
				return lastValue, lastEntryOffset, nil
			case -1:
				lastValue = values[len(values)-1]
				lastEntryOffset = entryBlockOffset
			default:
				return values[index], entryBlockOffset, nil
			}
		case 1:
			keyBytes, valueBytes, keySizeLeft, valueSizeLeft, err = GetKeysType1(block, dataBlockCheck, boundIndex)
			if err != nil {
				panic(err)
			}
			if dataBlockCheck {
				tombstone = len(valueBytes) == 0 && valueSizeLeft == 0
			}

		case 2:
			keyBytesToAppend, valueBytesToAppend, keySizeLeftNew, valueSizeLeftNew, err := GetKeysType2(block, keySizeLeft, valueSizeLeft, tombstone, dataBlockCheck)
			if err != nil {
				panic(err)
			}
			keyBytes = append(keyBytes, keyBytesToAppend...)
			valueBytes = append(valueBytes, valueBytesToAppend...)
			keySizeLeft = keySizeLeftNew
			valueSizeLeft = valueSizeLeftNew

		case 3:
			keyBytesToAppend, valueBytesToAppend, err := GetKeysType3(block, keySizeLeft, valueSizeLeft, tombstone, dataBlockCheck)
			if err != nil {
				panic(err)
			}
			keyBytes = append(keyBytes, keyBytesToAppend...)
			valueBytes = append(valueBytes, valueBytesToAppend...)

			var compareResult int
			if prefix {
				prefixFlag := bytes.HasPrefix(keyBytes, key)
				if prefixFlag {
					compareResult = 0
				} else {
					compareResult = bytes.Compare(keyBytes, key)
				}
			} else {
				compareResult = bytes.Compare(keyBytes, key)
			}

			if compareResult == 0 {
				return valueBytes, entryBlockOffset, nil // exact match
			} else if compareResult > 0 {
				if dataBlockCheck {
					return nil, 0, nil
				}
				if len(lastValue) == 0 {
					return nil, 0, nil
				}
				return lastValue, lastEntryOffset, nil // return offset of last value
			}
			lastValue = valueBytes
			lastEntryOffset = entryBlockOffset // track last value's offset

			keyBytes = keyBytes[:0]
			valueBytes = valueBytes[:0]
			keySizeLeft = 0
			valueSizeLeft = 0
			tombstone = false
		}
		blockOffset++
	}
	return lastValue, lastEntryOffset, nil
}

// Takes key and returns the value associated with the key as a byte slice.
// Returns [] if the key was not found
func SearchAll(key []byte, prefix bool) []byte {
	dataPath := getDataPath()
	readOrder := GetReadOrder(dataPath)
	for _, filePath := range readOrder {
		valueBytes, _ := SearchOne(filePath, key, prefix)
		if valueBytes != nil {
			return valueBytes
		}
	}
	return []byte{}
}

// Takes filepath of an ssTable and the key it should search for
// Returns the value of that key
// Return value will be empty slice if the entry found was tombstoned
// If the entry was not found return is nil, 0
func SearchOne(filePath string, key []byte, prefix bool) ([]byte, uint64) {
	dataPath := getDataPath()
	fileSeparator := string(filepath.Separator)
	var offset uint64
	var valueBytes []byte
	var err error
	var entryOffset uint64 = 0

	filePathSplit := strings.Split(filePath, fileSeparator)
	fileName := filePathSplit[len(filePathSplit)-1]
	fileLevel := filePathSplit[len(filePathSplit)-2]

	if !strings.HasSuffix(fileName, "compact.bin") {
		filePrefix, found := strings.CutSuffix(fileName, "-data.bin")
		if !found {
			panic(fmt.Sprintf("wrong file suffix, expected data.bin or compact.bin, got %s", fileName))
		}

		fileName = filePrefix + "-filter.bin"
		filePath := filepath.Join(dataPath, fileLevel, fileName)
		fileptr, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
		if err != nil {
			panic(err)
		}
		defer fileptr.Close()

		filter, _ := fetchFilter(fileptr, 0)
		if !bloomFilter.SearchData(filter, string(key)) {
			return nil, 0
		}

		fileName = filePrefix + "-summary.bin"
		filePath = filepath.Join(dataPath, fileLevel, fileName)
		valueBytes, _, err = searchSeparated(filePath, key, 0, prefix)
		if err != nil {
			panic(err)
		}
		if valueBytes == nil {
			return nil, 0
		}
		fileName = filePrefix + "-index.bin"
		filePath = filepath.Join(dataPath, fileLevel, fileName)

		if !config.VariableHeader {
			offset = binary.LittleEndian.Uint64(valueBytes)
		} else {
			offset, _ = binary.Uvarint(valueBytes)
		}
		valueBytes, _, err = searchSeparated(filePath, key, offset, prefix)
		if err != nil {
			panic(err)
		}

		fileName = filePrefix + "-data.bin"
		filePath = filepath.Join(dataPath, fileLevel, fileName)

		if !config.VariableHeader {
			offset = binary.LittleEndian.Uint64(valueBytes)
		} else {
			offset, _ = binary.Uvarint(valueBytes)
		}
		valueBytes, entryOffset, err = searchSeparated(filePath, key, offset, prefix)
		if err != nil {
			panic(err)
		}
		if valueBytes != nil {
			return valueBytes, entryOffset
		}
	} else {
		valueBytes, entryOffset, err = searchCompact(filePath, key, prefix)
		if err != nil {
			panic(err)
		}
		if valueBytes != nil {
			return valueBytes, entryOffset
		}
	}

	return nil, 0
}

// Last element is the maximum bound for the sstable
// This function returns this key in []byte
// Takes either "summary.bin" or "compact.bin"
func GetMaximumBound(summaryFile *os.File) ([]byte, error) {
	boundStart := getBoundIndex(summaryFile)
	block := blockManager.ReadBlock(summaryFile, uint64(boundStart))
	if block == nil {
		return nil, errors.New("block can not be nil")
	}
	if block.GetType() == BlockTypeMiddle || block.GetType() == BlockTypeEnd {
		return nil, errors.New("block should be type 1 or 0")
	}
	keySize := uint64(0)
	keyBytes := make([]byte, 0)
	data := fetchData(block)
	if !config.VariableHeader {
		keySizeBytes := data[:8]
		keySize = binary.LittleEndian.Uint64(keySizeBytes)
		keyBytes = append(keyBytes, data[8:min(8+keySize, uint64(len(data)))]...) // key value of the last element
	} else {
		n := 0
		keySize, n = binary.Uvarint(data)
		keyBytes = append(keyBytes, data[n:min(uint64(n)+keySize, uint64(len(data)))]...)
	}

	boundStart += 1
	if block.GetType() == 0 {
		return keyBytes, nil
	}
	for {
		block := blockManager.ReadBlock(summaryFile, uint64(boundStart))
		data = fetchData(block)
		keyBytes = append(keyBytes, data...)
		if block.GetType() == 3 {
			break
		}
		boundStart += 1
	}
	return keyBytes, nil

}

// Returns data of the block not including header or padding
func fetchData(block *blockManager.Block) []byte {
	blockData := block.GetData()
	blockHeader := 9
	dataSizeBytes := blockData[5 : 5+4]
	dataSize := binary.LittleEndian.Uint32(dataSizeBytes)
	data := blockData[blockHeader : blockHeader+int(dataSize)]
	return data
}

// Takes file that is either "compact.bin" or "summary.bin". It reads the index where
// maximum bound element starts and returns it
func getBoundIndex(file *os.File) uint64 {
	fileName := file.Name()
	fileInfo, err := file.Stat()
	if err != nil {
		panic(err)
	}
	fileSize := fileInfo.Size()
	footerBlockIndex := math.Ceil(float64(fileSize)/float64(config.GlobalBlockSize)) - 1

	footerBlock := blockManager.ReadBlock(file, uint64(footerBlockIndex))
	data := fetchData(footerBlock)
	switch {
	case strings.HasSuffix(fileName, "compact.bin"):
		if len(data) < 8*4 {
			panic("footer doesn't have all indices")
		}
		return binary.LittleEndian.Uint64(data[8*3 : 8*4])
	case strings.HasSuffix(fileName, "summary.bin"):
		if len(data) < 8*2 {
			panic("footer doesn't have all indices")
		}
		return binary.LittleEndian.Uint64(data[8*1 : 8*2])
	default:
		panic("file doesn't have footer")
	}
}

// Returns index if element is found in string slice;
// Returns index of last element that is lesser than the query
// If element is not found and search should continue return -1;
// If element is not found and search should not continue return -2
func FindLastSmallerKey(key []byte, keys [][]byte, dataBlock bool, prefix bool) int64 {
	for i := int64(0); i < int64(len(keys)); i++ {
		if prefix {
			prefixFlag := bytes.HasPrefix(keys[i], key)
			if prefixFlag {
				return i
			}
		}

		result := bytes.Compare(key, keys[i])
		if result == 0 {
			return i
		} else if result < 0 {
			if i != 0 {
				if !dataBlock {
					return i - 1
				} else {
					return -2
				}
			} else {
				// this can only happen when checking with the first key in segment
				return -2
			}
		}
	}
	return -1
}

func getFooter(file *os.File) *blockManager.Block {
	fileInfo, err := file.Stat()
	if err != nil {
		panic(err)
	}
	fileSize := fileInfo.Size()
	footerBlockIndex := math.Ceil(float64(fileSize)/float64(config.GlobalBlockSize)) - 1
	return blockManager.ReadBlock(file, uint64(footerBlockIndex))
}
