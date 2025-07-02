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
)

func findCompact(filePath string, key []byte) ([]byte, error) {
	if filePath[len(filePath)-11:] != "compact.bin" {
		panic("Error: findCompact only works on compact sstables")
	}

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		panic("Error: can't get file info")
	}

	var indexStart uint64
	var summaryStart uint64

	footerBlock := blockManager.ReadBlock(file, uint64(math.Ceil(float64(fileInfo.Size())/float64(blockSize))-1))
	footerData := footerBlock.GetData()

	//footerStart := binary.LittleEndian.Uint64(footerData[9:17])
	indexStart = binary.LittleEndian.Uint64(footerData[17:25])
	summaryStart = binary.LittleEndian.Uint64(footerData[25:33])
	maximumBound, err := getMaximumBound(file)
	boundIndex := getBoundIndex(file)
	if err != nil {
		panic(err)
	}

	if bytes.Compare(maximumBound, key) == -1 {
		return nil, nil
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

	for {
		if blockOffset == boundIndex {
			if !config.VariableEncoding {
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
		block := blockManager.ReadBlock(file, blockOffset)
		if block == nil {
			break
		} else if block.GetType() == 0 {
			keys, values, err = getKeysType0(block, dataBlockCheck, boundIndex)
			if err != nil {
				panic("Error reading block")
			}

			index := FindLastSmallerKey(key, keys, dataBlockCheck)
			if index == -1 {
				if dataBlockCheck {
					blockOffset += 1
					continue
				}
				currentSection += 1
				if !config.VariableEncoding {
					blockOffset = binary.LittleEndian.Uint64(values[len(values)-1])
				} else {
					blockOffset, _ = binary.Uvarint(values[len(values)-1])
				}
				continue
			} else if index == -2 {
				if dataBlockCheck {
					return nil, nil
				}
				if lastValue == nil {
					return nil, nil
				}
				if !config.VariableEncoding {
					blockOffset = binary.LittleEndian.Uint64(lastValue)
				} else {
					blockOffset, _ = binary.Uvarint(lastValue)
				}
				currentSection += 1
				lastValue = nil
				continue
			} else if dataBlockCheck {
				return values[index], nil
			}

			if !config.VariableEncoding {
				blockOffset = binary.LittleEndian.Uint64(values[index])
			} else {
				blockOffset, _ = binary.Uvarint(values[index])
			}
			currentSection += 1
			lastValue = nil
			continue

		} else if block.GetType() == 1 {
			keyBytes, valueBytes, keySizeLeft, valueSizeLeft, err = getKeysType1(block, dataBlockCheck, boundIndex)
			if err != nil {
				panic(err)
			}

			if dataBlockCheck {
				if len(valueBytes) == 0 && valueSizeLeft == 0 {
					tombstone = true
				} else {
					tombstone = false
				}
			}
			blockOffset += 1
		} else if block.GetType() == 2 {
			keyBytesToAppend, valueBytesToAppend, keySizeLeftNew, valueSizeLeftNew, err := getKeysType2(block, keySizeLeft, valueSizeLeft, tombstone, dataBlockCheck)
			if err != nil {
				panic(err)
			}

			keyBytes = append(keyBytes, keyBytesToAppend...)
			valueBytes = append(valueBytes, valueBytesToAppend...)
			keySizeLeft = keySizeLeftNew
			valueSizeLeft = valueSizeLeftNew
			blockOffset += 1
		} else if block.GetType() == 3 {
			keyBytesToAppend, valueBytesToAppend, err := getKeysType3(block, keySizeLeft, valueSizeLeft, tombstone, dataBlockCheck)
			if err != nil {
				panic(err)
			}

			keyBytes = append(keyBytes, keyBytesToAppend...)
			valueBytes = append(valueBytes, valueBytesToAppend...)

			compareResult := bytes.Compare(keyBytes, key)
			if compareResult == 0 {
				if dataBlockCheck {
					return valueBytes, nil
				}
				if !config.VariableEncoding {
					blockOffset = binary.LittleEndian.Uint64(valueBytes)
				} else {
					blockOffset, _ = binary.Uvarint(valueBytes)
				}
				currentSection += 1

				lastValue = nil
				keyBytes = keyBytes[:0]
				valueBytes = valueBytes[:0]
				keySizeLeft = 0
				valueSizeLeft = 0
				tombstone = false
				continue
			} else if compareResult > 0 {
				if dataBlockCheck {
					return nil, nil
				}
				if lastValue == nil {
					return nil, nil
				}
				if !config.VariableEncoding {
					blockOffset = binary.LittleEndian.Uint64(lastValue)
				} else {
					blockOffset, _ = binary.Uvarint(lastValue)
				}
				currentSection += 1

				lastValue = nil
				keyBytes = keyBytes[:0]
				valueBytes = valueBytes[:0]
				keySizeLeft = 0
				valueSizeLeft = 0
				tombstone = false
				continue
			} else if compareResult < 0 && !dataBlockCheck {
				if !config.VariableEncoding {
					blockOffset = binary.LittleEndian.Uint64(valueBytes)
				} else {
					blockOffset, _ = binary.Uvarint(valueBytes)
				}
				currentSection += 1

				lastValue = nil
				keyBytes = keyBytes[:0]
				valueBytes = valueBytes[:0]
				keySizeLeft = 0
				valueSizeLeft = 0
				tombstone = false
				continue
			}
			blockOffset += 1
			lastValue = valueBytes
			keyBytes = keyBytes[:0]
			valueBytes = valueBytes[:0]
			keySizeLeft = 0
			valueSizeLeft = 0
			tombstone = false
		}
	}

	return nil, nil
}

// When matching key is found, returns its value
// If key stops being larger than comparing key, return value of last key
// Returns nil if not found
func findSeparated(filePath string, key []byte, offset uint64) ([]byte, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
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
	var blockOffset uint64 = offset

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
		} else if block.GetType() == 0 {
			keys, values, err = getKeysType0(block, dataBlockCheck, boundIndex)
			if err != nil {
				panic(err)
			}

			index := FindLastSmallerKey(key, keys, dataBlockCheck)
			if index == -2 {
				if dataBlockCheck {
					return nil, nil
				}
				if len(lastValue) == 0 {
					return nil, nil
				}
				return lastValue, nil
			}
			if index != -1 {
				return values[index], nil
			}
			lastValue = values[len(values)-1]

		} else if block.GetType() == 1 {
			keyBytes, valueBytes, keySizeLeft, valueSizeLeft, err = getKeysType1(block, dataBlockCheck, boundIndex)
			if err != nil {
				panic(err)
			}

			if dataBlockCheck {
				if len(valueBytes) == 0 && valueSizeLeft == 0 {
					tombstone = true
				} else {
					tombstone = false
				}
			}

		} else if block.GetType() == 2 {
			keyBytesToAppend, valueBytesToAppend, keySizeLeftNew, valueSizeLeftNew, err := getKeysType2(block, keySizeLeft, valueSizeLeft, tombstone, dataBlockCheck)
			if err != nil {
				panic(err)
			}

			keyBytes = append(keyBytes, keyBytesToAppend...)
			valueBytes = append(valueBytes, valueBytesToAppend...)
			keySizeLeft = keySizeLeftNew
			valueSizeLeft = valueSizeLeftNew
		} else if block.GetType() == 3 {
			keyBytesToAppend, valueBytesToAppend, err := getKeysType3(block, keySizeLeft, valueSizeLeft, tombstone, dataBlockCheck)
			if err != nil {
				panic(err)
			}

			keyBytes = append(keyBytes, keyBytesToAppend...)
			valueBytes = append(valueBytes, valueBytesToAppend...)

			compareResult := bytes.Compare(keyBytes, key)

			if compareResult == 0 {
				return valueBytes, nil
			} else if compareResult > 0 {
				if dataBlockCheck {
					return nil, nil
				}
				if len(lastValue) == 0 {
					return nil, nil
				}
				return lastValue, nil
			}
			lastValue = valueBytes
			keyBytes = keyBytes[:0]
			valueBytes = valueBytes[:0]
			keySizeLeft = 0
			valueSizeLeft = 0
			tombstone = false
		}
		blockOffset += 1
	}

	return lastValue, nil
}

// Takes key and returns the value associated with the key as a byte slice.
// Returns [] if the key was not found
func Find(key []byte) []byte {
	dataPath := getDataPath()
	fileSeparator := string(filepath.Separator)
	var offset uint64
	var valueBytes []byte
	var err error
	iterator := 1
	readOrder := GetReadOrder(dataPath)
	for _, file := range readOrder {
		iterator += 1
		filePathSplit := strings.Split(file, fileSeparator)
		fileName := filePathSplit[len(filePathSplit)-1]
		fileLevel := filePathSplit[len(filePathSplit)-2]
		if !strings.HasSuffix(fileName, "compact.bin") {
			filePrefix, found := strings.CutSuffix(fileName, "-data.bin")
			if !found {
				panic(fmt.Sprintf("wrong file suffix, expected data.bin or compact.bin, got %s", fileName))
			}
			fileName = filePrefix + "-summary.bin"
			filePath := filepath.Join(dataPath, fileLevel, fileName)
			valueBytes, err = findSeparated(filePath, key, 0)
			if err != nil {
				panic(err)
			}
			if valueBytes == nil {
				continue
			}
			fileName = filePrefix + "-index.bin"
			filePath = filepath.Join(dataPath, fileLevel, fileName)

			if !config.VariableEncoding {
				offset = binary.LittleEndian.Uint64(valueBytes)
			} else {
				offset, _ = binary.Uvarint(valueBytes)
			}
			valueBytes, err = findSeparated(filePath, key, offset)
			if err != nil {
				panic(err)
			}

			fileName = filePrefix + "-data.bin"
			filePath = filepath.Join(dataPath, fileLevel, fileName)

			if !config.VariableEncoding {
				offset = binary.LittleEndian.Uint64(valueBytes)
			} else {
				offset, _ = binary.Uvarint(valueBytes)
			}
			valueBytes, err = findSeparated(filePath, key, offset)
			if err != nil {
				panic(err)
			}
			if valueBytes != nil {
				return valueBytes
			}

		} else {
			valueBytes, err = findCompact(file, key)
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

// Last element is the maximum bound for the sstable
// This function returns this key in []byte
func getMaximumBound(summaryFile *os.File) ([]byte, error) {
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
	if !config.VariableEncoding {
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
func FindLastSmallerKey(key []byte, keys [][]byte, dataBlock bool) int64 {
	for i := int64(0); i < int64(len(keys)); i++ {
		compareResult := bytes.Compare(key, keys[i])
		if compareResult == 0 {
			return i
		} else if compareResult == -1 {
			if i != 0 && !dataBlock {
				return i - 1
			} else {
				return -2
			}
		}
	}
	return -1
}
