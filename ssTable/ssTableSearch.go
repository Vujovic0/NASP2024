package ssTable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/Vujovic0/NASP2024/blockManager"
	"github.com/Vujovic0/NASP2024/config"
)

func ReadFooter(file *os.File) (footerStart, indexStart, summaryStart, boundStart uint64, err error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, 0, 0, 0, err
	}

	lastBlockOffset := uint64(math.Ceil(float64(fileInfo.Size())/float64(config.GlobalBlockSize))) - 1
	footerBlock := blockManager.ReadBlock(file, lastBlockOffset)
	if footerBlock == nil {
		return 0, 0, 0, 0, errors.New("footer block not found")
	}
	footerData := footerBlock.GetData()

	if len(footerData) < 32 {
		return 0, 0, 0, 0, errors.New("footer data too short")
	}

	footerStart = binary.LittleEndian.Uint64(footerData[0:8])
	indexStart = binary.LittleEndian.Uint64(footerData[8:16])
	summaryStart = binary.LittleEndian.Uint64(footerData[16:24])
	boundStart = binary.LittleEndian.Uint64(footerData[24:32])

	return footerStart, indexStart, summaryStart, boundStart, nil
}

func searchCompact(filePath string, key []byte, prefix bool) ([]byte, uint64, error) {
	if !strings.HasSuffix(filePath, "compact.bin") {
		panic("Error: searchCompact only works on compact sstables")
	}

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()

	_, indexStart, summaryStart, boundStart, err := ReadFooter(file)
	if err != nil {
		return nil, 0, err
	}

	maximumBound, err := getMaximumBound(file)
	if err != nil {
		return nil, 0, err
	}

	if bytes.Compare(maximumBound, key) < 0 {
		fmt.Printf("[DEBUG] Key %s is beyond maximum bound %s\n", string(key), string(maximumBound))
		return nil, 0, nil
	}

	blockOffset := summaryStart
	dataBlockCheck := false

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
		if currentSection == 0 && blockOffset >= boundStart {
			currentSection = 1
			dataBlockCheck = true
			fmt.Printf("[DEBUG] Switching to data block section at offset %d\n", blockOffset)
			continue
		}
		if currentSection == 1 && blockOffset >= summaryStart {
			currentSection = 2
			fmt.Printf("[DEBUG] Switching to summary section at offset %d\n", blockOffset)
			continue
		}
		if currentSection == 2 && blockOffset >= indexStart {
			fmt.Printf("[DEBUG] Reached index start at offset %d, breaking\n", blockOffset)
			break
		}

		entryBlockOffset := blockOffset
		block := blockManager.ReadBlock(file, blockOffset)
		if block == nil {
			fmt.Printf("[DEBUG] No block found at offset %d, breaking\n", blockOffset)
			break
		}

		fmt.Printf("[DEBUG] Reading block at offset %d, type %d\n", blockOffset, block.GetType())

		switch block.GetType() {
		case 0:
			keys, values, err = GetKeysType0(block, dataBlockCheck, boundStart)
			if err != nil {
				panic("Error reading block type 0: " + err.Error())
			}
			fmt.Printf("[DEBUG] Block type 0: Found %d keys\n", len(keys))

			index := FindLastSmallerKey(key, keys, dataBlockCheck, prefix)
			fmt.Printf("[DEBUG] FindLastSmallerKey returned index %d for key %s\n", index, string(key))

			if index == -1 {
				if dataBlockCheck {
					blockOffset++
					continue
				}
				currentSection++
				// Decode offset safely
				if !config.VariableEncoding {
					if len(values) == 0 || len(values[len(values)-1]) < 8 {
						panic("Invalid value length for offset decoding")
					}
					blockOffset = binary.LittleEndian.Uint64(values[len(values)-1][:8])
				} else {
					var n int
					blockOffset, n = binary.Uvarint(values[len(values)-1])
					if n <= 0 {
						panic("Failed to decode varint offset")
					}
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
				fmt.Printf("[DEBUG] Found value at index %d in data block\n", index)
				return values[index], entryBlockOffset, nil
			}

			// Not in data block, decode offset for next search
			if !config.VariableEncoding {
				if len(values[index]) < 8 {
					panic("Invalid value length for offset decoding")
				}
				blockOffset = binary.LittleEndian.Uint64(values[index][:8])
			} else {
				var n int
				blockOffset, n = binary.Uvarint(values[index])
				if n <= 0 {
					panic("Failed to decode varint offset")
				}
			}
			currentSection++
			lastValue = nil
			continue

		case 1:
			keyBytes, valueBytes, keySizeLeft, valueSizeLeft, err = GetKeysType1(block, dataBlockCheck, boundStart)
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
				if bytes.HasPrefix(keyBytes, key) {
					compareResult = 0
				} else {
					compareResult = bytes.Compare(keyBytes, key)
				}
			} else {
				compareResult = bytes.Compare(keyBytes, key)
			}

			if compareResult == 0 {
				if dataBlockCheck {
					fmt.Printf("[DEBUG] Found matching key in data block at offset %d\n", entryBlockOffset)
					return valueBytes, entryBlockOffset, nil
				}
				// decode next offset for next section search
				if !config.VariableEncoding {
					if len(valueBytes) < 8 {
						panic("Invalid valueBytes length for offset decoding")
					}
					blockOffset = binary.LittleEndian.Uint64(valueBytes[:8])
				} else {
					var n int
					blockOffset, n = binary.Uvarint(valueBytes)
					if n <= 0 {
						panic("Failed to decode varint offset")
					}
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
				// decode next offset
				if !config.VariableEncoding {
					if len(valueBytes) < 8 {
						panic("Invalid valueBytes length for offset decoding")
					}
					blockOffset = binary.LittleEndian.Uint64(valueBytes[:8])
				} else {
					var n int
					blockOffset, n = binary.Uvarint(valueBytes)
					if n <= 0 {
						panic("Failed to decode varint offset")
					}
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

	fmt.Printf("[DEBUG] Key %s not found\n", string(key))
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
	var lastValue []byte = nil

	var keySizeLeft uint64 = 0
	var valueSizeLeft uint64 = 0

	var boundIndex uint64 = 0
	var entryBlockOffset uint64 = 0
	var lastEntryOffset uint64 = 0

	fileType := filePath[len(filePath)-11:]
	if fileType == "data.bin" {
		maximumBound, err := getMaximumBound(file)
		if err != nil {
			return nil, 0, err
		}
		if bytes.Compare(key, maximumBound) > 0 {
			// Ako je traženi ključ veći od maksimuma, nema ga u tabeli
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
		entryBlockOffset = block.GetOffset()

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
				if lastValue == nil {
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
				if bytes.HasPrefix(keyBytes, key) {
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
				return nil, 0, errors.New("key found in non-data block which is unexpected")
			} else if compareResult > 0 {
				if dataBlockCheck {
					return nil, 0, nil
				}
				if lastValue == nil {
					return nil, 0, nil
				}
				return lastValue, lastEntryOffset, nil
			}

			lastValue = valueBytes
			lastEntryOffset = entryBlockOffset

		}

		blockOffset++
	}

	return nil, 0, nil
}

// Takes key and returns the value associated with the key as a byte slice.
// Returns [] if the key was not found
func SearchAll(key []byte, prefix bool) []byte {
	dataPath := getDataPath()
	readOrder := GetReadOrder(dataPath)

	fmt.Println("SearchAll read order:")
	for _, f := range readOrder {
		fmt.Println("  -", f)
	}

	for _, filePath := range readOrder {
		valueBytes, _, err := SearchOne(filePath, key, prefix)
		if err != nil {
			fmt.Printf("Error searching key %s in file %s: %v\n", string(key), filePath, err)
			continue
		}
		if valueBytes != nil {
			fmt.Printf("Found key %s in file %s\n", string(key), filePath)
			return valueBytes
		}
	}
	return nil
}

func SearchOne(filePath string, key []byte, prefix bool) ([]byte, uint64, error) {
	dataPath := getDataPath()
	fileSeparator := string(filepath.Separator)
	var offset uint64
	var entryOffset uint64
	var valueBytes []byte
	var err error

	fmt.Printf("Searching in file: %s\n", filePath)

	filePathSplit := strings.Split(filePath, fileSeparator)
	fileName := filePathSplit[len(filePathSplit)-1]
	fileLevel := filePathSplit[len(filePathSplit)-2]

	if strings.HasSuffix(fileName, "compact.bin") {
		valueBytes, entryOffset, err = searchCompact(filePath, key, prefix)
		if err != nil {
			fmt.Printf("searchCompact error in %s: %v\n", filePath, err)
			return nil, 0, fmt.Errorf("searchCompact error: %w", err)
		}
		fmt.Printf("searchCompact found value? %v\n", valueBytes != nil)
		return valueBytes, entryOffset, nil
	}

	filePrefix, found := strings.CutSuffix(fileName, "-data.bin")
	if !found {
		return nil, 0, fmt.Errorf("wrong file suffix, expected data.bin or compact.bin, got %s", fileName)
	}

	summaryPath := filepath.Join(dataPath, fileLevel, filePrefix+"-summary.bin")
	summaryBytes, _, err := searchSeparated(summaryPath, key, 0, prefix)
	if err != nil {
		return nil, 0, fmt.Errorf("searchSeparated summary error: %w", err)
	}
	if summaryBytes == nil {
		// Nije pronađeno u summary, znači nema tog ključa
		fmt.Printf("Key %s not found in summary file %s\n", string(key), summaryPath)
		return nil, 0, nil
	}

	indexPath := filepath.Join(dataPath, fileLevel, filePrefix+"-index.bin")
	if !config.VariableEncoding {
		offset = binary.LittleEndian.Uint64(summaryBytes)
	} else {
		offset, _ = binary.Uvarint(summaryBytes)
	}
	indexBytes, _, err := searchSeparated(indexPath, key, offset, prefix)
	if err != nil {
		return nil, 0, fmt.Errorf("searchSeparated index error: %w", err)
	}

	dataPathFinal := filepath.Join(dataPath, fileLevel, filePrefix+"-data.bin")
	if !config.VariableEncoding {
		offset = binary.LittleEndian.Uint64(indexBytes)
	} else {
		offset, _ = binary.Uvarint(indexBytes)
	}
	valueBytes, entryOffset, err = searchSeparated(dataPathFinal, key, offset, prefix)
	if err != nil {
		return nil, 0, fmt.Errorf("searchSeparated data error: %w", err)
	}

	return valueBytes, entryOffset, nil
}

// Last element is the maximum bound for the sstable
// This function returns this key in []byte
func getMaximumBound(file *os.File) ([]byte, error) {
	// Velicina footera = samo 8 bajtova (int64 offset do bloka sa max ključem)
	const footerSize = 8

	// Idi na kraj fajla - 8 bajtova
	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fi.Size()
	if fileSize < footerSize {
		return nil, errors.New("file too small for footer")
	}

	_, err = file.Seek(-footerSize, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	var maxKeyOffset int64
	err = binary.Read(file, binary.LittleEndian, &maxKeyOffset)
	if err != nil {
		return nil, err
	}

	// Pročitaj blok na tom offsetu
	block := blockManager.ReadBlock(file, uint64(maxKeyOffset))
	if block == nil {
		return nil, errors.New("block is nil")
	}

	data := fetchData(block)

	// Parsiraj keySize
	keySize, n := binary.Varint(data)
	if n <= 0 {
		return nil, errors.New("failed to decode keySize")
	}

	// Izdvoji ključ
	if int(keySize)+n > len(data) {
		return nil, errors.New("key out of range")
	}
	keyBytes := data[n : n+int(keySize)]
	return keyBytes, nil
}

// Dodajemo proveru u fetchData da ne bi prelazilo preko veličine slice-a:
func fetchData(block *blockManager.Block) []byte {
	blockData := block.GetData()
	blockHeader := 9
	if len(blockData) < blockHeader+4 {
		fmt.Printf("[fetchData] Warning: block data length %d too short for header + size\n", len(blockData))
		return nil
	}
	dataSizeBytes := blockData[5 : 5+4]
	dataSize := binary.LittleEndian.Uint32(dataSizeBytes)
	if int(blockHeader+int(dataSize)) > len(blockData) {
		fmt.Printf("[fetchData] Warning: dataSize %d exceeds block data length %d\n", dataSize, len(blockData))
		return nil
	}
	data := blockData[blockHeader : blockHeader+int(dataSize)]
	fmt.Printf("[fetchData] Returning data slice length: %d\n", len(data))
	return data
}

// Takes file that is either "compact.bin" or "summary.bin". It reads the index where
// maximum bound element starts and returns it
func getBoundIndex(file *os.File) uint64 {
	const footerSize = 8
	fi, err := file.Stat()
	if err != nil {
		return 0
	}
	if fi.Size() < footerSize {
		return 0
	}
	_, err = file.Seek(-footerSize, io.SeekEnd)
	if err != nil {
		return 0
	}
	var maxOffset int64
	err = binary.Read(file, binary.LittleEndian, &maxOffset)
	if err != nil {
		return 0
	}
	return uint64(maxOffset)
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
