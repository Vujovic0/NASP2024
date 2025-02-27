package ssTable

import (
	"NASP2024/blockManager"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"strconv"
)

var blockSize uint64 = 1024 * 4

type channelResult struct {
	Block *blockManager.Block
	Key   []byte
}

// Creates blocks for the SSTable without writing them. Works as a generator function when used with keyword "range".
// Takes a byte array formed by entries with certain formatting, file path,
// check if the blocks are data blocks and the starting offset of a block in a file
// If the blocks are data blocks, then 21 bytes are skipped when reading keysize and valuesize
// | CRC 4B | TimeStamp 16B | Tombstone 1B | Keysize 8B | Valuesize 8B | Key... | Value... |
// If entry has tombstone 1, the entry MUST NOT have valueSize and value
// Returns block per block as well as the first key in the block in bytes
func PrepareSSTableBlocks(filePath string, data []byte, dataBlocksCheck bool, blockOffset uint64) <-chan *channelResult {
	ch := make(chan *channelResult)

	process := func() {

		var keySizeOffset int = 0
		if dataBlocksCheck {
			keySizeOffset = 21
		}

		if len(data) < keySizeOffset+16 {
			panic("Invalid data")
		}

		var dataPointer uint64 = 0
		//skips the header of the block |CRC 4B|Type 1B|Data size 4B|
		blockPointer := 9
		blockData := make([]byte, blockSize)

		var crcValue uint32 = 0
		var blockType byte = 0

		var entryValueSize uint64 = 0
		var entryKeySize uint64 = 0
		var entrySize uint64 = 0
		var entrySizeLeft uint64 = 0
		var block *blockManager.Block
		var newEntryCheck bool = true
		var newBlockCheck bool = true
		var key []byte

		for dataPointer < uint64(len(data)) {

			if newEntryCheck {
				entryKeySize = binary.BigEndian.Uint64(data[dataPointer+uint64(keySizeOffset) : dataPointer+uint64(keySizeOffset)+8])
				//dataPointer+20 represents the tombstone byte only in data segments, not index or summary
				if dataBlocksCheck && data[dataPointer+20] == 1 {
					entryValueSize = 0
				} else {
					entryValueSize = binary.BigEndian.Uint64(data[dataPointer+uint64(keySizeOffset)+8 : dataPointer+uint64(keySizeOffset)+16])
				}
				entrySize = entryKeySize + entryValueSize + uint64(keySizeOffset) + 16
				entrySizeLeft = entrySize
				if newBlockCheck {
					key = data[dataPointer+uint64(keySizeOffset)+16 : dataPointer+uint64(keySizeOffset)+16+entryKeySize]
					newBlockCheck = false
				}
			}

			if blockSize-uint64(blockPointer) >= entrySize {
				copy(blockData[blockPointer:], data[dataPointer:uint64(dataPointer)+entrySize])
				dataPointer += entrySize
				blockPointer += int(entrySize)
				if dataPointer == uint64(len(data)) {
					blockType = 0

					blockData[4] = blockType
					binary.BigEndian.PutUint32(blockData[5:9], uint32(blockPointer-9))

					crcValue = crc32.ChecksumIEEE(blockData[4:])
					binary.BigEndian.PutUint32(blockData[0:4], crcValue)

					block = blockManager.InitBlock(filePath, blockOffset, blockData)
					ch <- &channelResult{Block: block, Key: key}
					break
				}
				continue
			} else if blockPointer != 9 {
				blockType = 0

				blockData[4] = blockType
				binary.BigEndian.PutUint32(blockData[5:9], uint32(blockPointer-9))

				crcValue = crc32.ChecksumIEEE(blockData[4:])
				binary.BigEndian.PutUint32(blockData[0:4], crcValue)

				block = blockManager.InitBlock(filePath, blockOffset, blockData)
			} else {
				if newEntryCheck {
					blockType = 1
				} else {
					blockType = 2
				}
				newEntryCheck = false
				if uint64(blockSize-9) >= entrySizeLeft {
					blockType = 3

					blockData[4] = blockType
					copy(blockData[9:], data[dataPointer:uint64(dataPointer)+entrySizeLeft])
					binary.BigEndian.PutUint32(blockData[5:9], uint32(entrySizeLeft))

					crcValue = crc32.ChecksumIEEE(blockData[4:])
					binary.BigEndian.PutUint32(blockData[0:4], crcValue)

					block = blockManager.InitBlock(filePath, blockOffset, blockData)
					newEntryCheck = true
					dataPointer += entrySizeLeft
				} else {
					entrySizeLeft -= uint64(blockSize - 9)

					binary.BigEndian.PutUint32(blockData[5:9], uint32(blockSize-9))
					blockData[4] = blockType
					copy(blockData[9:], data[dataPointer:uint64(dataPointer)+uint64(blockSize-9)])

					crcValue = crc32.ChecksumIEEE(blockData[4:])
					binary.BigEndian.PutUint32(blockData[0:4], crcValue)

					dataPointer += uint64(blockSize - 9)
					block = blockManager.InitBlock(filePath, blockOffset, blockData)
				}
			}
			blockData = make([]byte, blockSize)
			blockPointer = 9
			blockOffset += 1
			ch <- &channelResult{Block: block, Key: key}
			newBlockCheck = true
		}
	}

	go func() {
		process()
		close(ch)
	}()

	return ch
}

// Entires of summary and index are of fomrat |KEYSIZE|VALUESIZE|KEY|VALUE|
// First entry of summary is |KEYSIZE|0|KEY|0| and it's used to check if element is out of bounds
// Entires of data are |CRC|TIMESTAMP|TOMBSTONE|KEYSIZE|VALUZESIZE|KEY|VALUE
func CreateSeparatedSSTable(data []byte, lastElementData []byte, summary_sparsity int, index_sparsity int) {
	generation := getGeneration()

	fileName := "data" + string(os.PathSeparator) + "L0" + string(os.PathSeparator) + "usertable-" + strconv.FormatUint(uint64(generation), 10)
	FILEDATAPATH := fileName + "-data.bin"
	FILEINDEXPATH := fileName + "-index.bin"
	FILESUMMARYPATH := fileName + "-summary.bin"

	fileData, err := os.OpenFile(FILEDATAPATH, os.O_CREATE, 0664)
	if err != nil {
		panic(err)
	}
	defer fileData.Close()

	fileIndex, err := os.OpenFile(FILEINDEXPATH, os.O_CREATE, 0664)
	if err != nil {
		panic(err)
	}
	defer fileIndex.Close()

	fileSummary, err := os.OpenFile(FILESUMMARYPATH, os.O_CREATE, 0664)
	if err != nil {
		panic(err)
	}
	defer fileSummary.Close()

	var indexData []byte
	var summaryData []byte
	summaryData = append(summaryData, lastElementData...)

	var keyBinary []byte
	valueBinary := make([]byte, 8)
	valueSizeBinary := []byte{0, 0, 0, 0, 0, 0, 0, 8}
	keySizeBinary := make([]byte, 8)
	var counter int = 0

	//Creates the data segment while preparing data for the index segment
	for channelResult := range PrepareSSTableBlocks(FILEDATAPATH, data, true, 0) {
		blockManager.WriteBlock(fileData, channelResult.Block)
		if !bytes.Equal(keyBinary, channelResult.Key) {
			if counter%index_sparsity == 0 {
				keyBinary = channelResult.Key
				binary.BigEndian.PutUint64(keySizeBinary, uint64(len(keyBinary)))
				binary.BigEndian.PutUint64(valueBinary, uint64(channelResult.Block.GetOffset()))
				indexData = append(indexData, keySizeBinary...)
				indexData = append(indexData, valueSizeBinary...)
				indexData = append(indexData, keyBinary...)
				indexData = append(indexData, valueBinary...)
			}
			counter += 1
		}
	}

	keyBinary = nil
	counter = 0

	//Creates the index segment while preparing data for the summary segment
	for channelResult := range PrepareSSTableBlocks(FILEINDEXPATH, indexData, false, 0) {
		blockManager.WriteBlock(fileIndex, channelResult.Block)
		if !bytes.Equal(keyBinary, channelResult.Key) {
			if counter%summary_sparsity == 0 {
				keyBinary = channelResult.Key
				binary.BigEndian.PutUint64(keySizeBinary, uint64(len(keyBinary)))
				binary.BigEndian.PutUint64(valueBinary, uint64(channelResult.Block.GetOffset()))
				summaryData = append(summaryData, keySizeBinary...)
				summaryData = append(summaryData, valueSizeBinary...)
				summaryData = append(summaryData, keyBinary...)
				summaryData = append(summaryData, valueBinary...)
			}
			counter += 1
		}
	}

	//Creates summary segment
	for channelResult := range PrepareSSTableBlocks(FILESUMMARYPATH, summaryData, false, 0) {
		blockManager.WriteBlock(fileSummary, channelResult.Block)
	}
}

func getGeneration() uint64 {
	var generation uint64

	_, err := os.Stat("data" + string(os.PathSeparator) + "metaData.bin")
	fileExists := err == nil || !os.IsNotExist(err)

	metaFile, err := os.OpenFile("data"+string(os.PathSeparator)+"metaData.bin", os.O_RDWR|os.O_CREATE, 0664)
	if err != nil {
		panic(err)
	}
	defer metaFile.Close()
	var block *blockManager.Block
	if fileExists {
		block = blockManager.ReadBlock(metaFile, 0)
		generation = binary.BigEndian.Uint64(block.GetData()[9:17])
		generation += 1
	} else {
		generation = 1
	}
	dataPlaceholder := make([]byte, blockSize)
	binary.BigEndian.PutUint32(dataPlaceholder[5:9], 8)
	binary.BigEndian.PutUint64(dataPlaceholder[9:17], generation)
	crcValue := crc32.ChecksumIEEE(dataPlaceholder[4:])
	binary.BigEndian.PutUint32(dataPlaceholder[:4], crcValue)
	block = blockManager.InitBlock("metaData.bin", 0, dataPlaceholder)
	blockManager.WriteBlock(metaFile, block)

	return generation
}

// When matching key is found, returns its value
// If key stops being larger than comparing key, return value of last key
// First key in summary is always the last key in data, so if the searched key is greater it means that it doesn't exist
// Returns nil if not found
func findSeparated(filePath string, key string) []byte {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		panic("File not found: " + filePath)
	}
	defer file.Close()

	var dataBlockCheck bool = true
	var tombstone bool = false

	var keys []string = make([]string, 0)
	var keyBytes []byte = make([]byte, 0)

	var valueBytes []byte = make([]byte, 0)
	var values [][]byte = make([][]byte, 0)
	var lastValue []byte = make([]byte, 0)

	var keySizeLeft uint64 = 0
	var valueSizeLeft uint64 = 0

	fileType := filePath[len(filePath)-11:]

	var firstKey bool = true

	if fileType == "summary.bin" {
		firstKey = false
	}

	var blockOffset uint64 = 0

	for true {
		block := blockManager.ReadBlock(file, blockOffset)
		if block == nil {
			break
		} else if block.GetType() == 0 {
			if fileType[len(fileType)-8:] == "data.bin" {
				dataBlockCheck = true
			} else {
				dataBlockCheck = false
			}
			keys, values, err = getKeysType0(block, dataBlockCheck)
			if err != nil {
				panic(err)
			}

			if firstKey {
				if key > keys[0] {
					return nil
				}
				firstKey = false
				keys = keys[1:]
				values = values[1:]
			}

			index := FindLastSmallerKey(key, keys)
			if index == -2 {
				if dataBlockCheck {
					return nil
				}
				return lastValue
			}
			if index != -1 {
				return values[index]
			}
			lastValue = values[len(values)-1]

		} else if block.GetType() == 1 {
			keyBytes, valueBytes, keySizeLeft, valueSizeLeft, err = getKeysType1(block, dataBlockCheck)
			if err != nil {
				panic(err)
			}

			if len(valueBytes) == 0 {
				tombstone = true
			} else {
				tombstone = false
			}

		} else if block.GetType() == 2 {
			keyBytesToAppend, valueBytesToAppend, keySizeLeftNew, valueSizeLeftNew, err := getKeysType2(block, keySizeLeft, valueSizeLeft, tombstone)
			if err != nil {
				panic(err)
			}

			keyBytes = append(keyBytes, keyBytesToAppend...)
			valueBytes = append(valueBytes, valueBytesToAppend...)
			keySizeLeft = keySizeLeftNew
			valueSizeLeft = valueSizeLeftNew
		} else if block.GetType() == 3 {
			keyBytesToAppend, valueBytesToAppend, err := getKeysType3(block, keySizeLeft, valueSizeLeft, tombstone)
			if err != nil {
				panic(err)
			}

			keyBytes = append(keyBytes, keyBytesToAppend...)
			valueBytes = append(valueBytes, valueBytesToAppend...)
			keyConstruct := string(keyBytes)

			if firstKey {
				firstKey = false
				if key > keyConstruct {
					return nil
				}
			}

			if keyConstruct == key {
				return valueBytes
			} else if key < keyConstruct {
				if dataBlockCheck {
					return nil
				}
				return lastValue
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

	return nil
}

// Returns a slice of keys and their values in bytes of a full block.
// DataBlockCheck is true when reading from a data segment in sstable because entries
// in summary and index don't have CRC, timestamp and tombstone
// If value of an entry is an empty slice, it means that the tombstone is set to 1
func getKeysType0(block *blockManager.Block, dataBlockCheck bool) ([]string, [][]byte, error) {
	blockData := block.GetData()
	dataSize := uint64(block.GetSize())
	var keyOffset int = 0
	var tombstone bool = false
	if dataBlockCheck {
		keyOffset = 21
	}
	blockPointer := uint64(9 + keyOffset)
	var keySlice []string
	var valueSlice [][]byte
	var valueSize uint64

	for blockPointer < dataSize {
		if dataBlockCheck && blockData[blockPointer-1] == 1 {
			tombstone = true
		}

		if blockPointer+8 > dataSize {
			return nil, nil, errors.New("The block is corrupted")
		}
		keySize := binary.BigEndian.Uint64(blockData[blockPointer : blockPointer+8])
		blockPointer += 8
		if blockPointer+8 > dataSize {
			return nil, nil, errors.New("The block is corrupted")
		}

		if tombstone {
			valueSize = 0
		} else {
			valueSize = binary.BigEndian.Uint64(blockData[blockPointer : blockPointer+8])
		}
		blockPointer += 8
		if blockPointer+keySize > dataSize {
			return nil, nil, errors.New("The block is corrupted")
		}
		keyBytes := blockData[blockPointer : blockPointer+keySize]
		blockPointer += keySize
		if blockPointer+valueSize > dataSize {
			return nil, nil, errors.New("The block is corrupted")
		}
		valueBytes := blockData[blockPointer : blockPointer+valueSize]
		blockPointer += valueSize
		keySlice = append(keySlice, string(keyBytes))
		valueSlice = append(valueSlice, valueBytes)
		tombstone = false
	}

	return keySlice, valueSlice, nil
}

// Returns key bytes, value bytes, key size left, value size left
func getKeysType1(block *blockManager.Block, dataBlockCheck bool) ([]byte, []byte, uint64, uint64, error) {
	blockData := block.GetData()
	dataSize := uint64(block.GetSize())
	var keyOffset int = 0
	if dataBlockCheck {
		keyOffset = 21
	}
	blockPointer := uint64(9 + keyOffset)
	var keyBytes []byte = make([]byte, 0)
	var valueBytes []byte = make([]byte, 0)
	var valueSize uint64 = 0
	var tombstone bool = false

	if dataBlockCheck && blockData[blockPointer-1] == 1 {
		tombstone = true
	}
	if blockPointer+8 > dataSize {
		return nil, nil, 0, 0, errors.New("The block is corrupted")
	}
	keySize := binary.BigEndian.Uint64(blockData[blockPointer : blockPointer+8])
	blockPointer += 8
	if blockPointer+8 > dataSize {
		return nil, nil, 0, 0, errors.New("The block is corrupted")
	}
	if !tombstone {
		valueSize = binary.BigEndian.Uint64(blockData[blockPointer : blockPointer+8])
	}
	blockPointer += 8

	//Check if key data fits inside or overflows further
	if blockPointer+keySize > blockSize {
		keyBytes = append(keyBytes, blockData[blockPointer:blockSize]...)
		keySize -= blockSize - blockPointer
		blockPointer = blockSize
	} else {
		keyBytes = append(keyBytes, blockData[blockPointer:blockPointer+keySize]...)
		blockPointer += keySize
		keySize = 0
	}

	if !tombstone {
		//If the value data fits, then it should be type 0
		if blockPointer+valueSize <= blockSize {
			return nil, nil, 0, 0, errors.New("The block is corrupted")
		}

		valueBytes = append(valueBytes, blockData[blockPointer:blockSize]...)
		valueSize -= blockSize - blockPointer
	}

	return keyBytes, valueBytes, keySize, valueSize, nil
}

// Returns key bytes, value bytes, key size left, value size left
func getKeysType2(block *blockManager.Block, keySize uint64, valueSize uint64, tombstone bool) ([]byte, []byte, uint64, uint64, error) {
	blockData := block.GetData()
	blockPointer := uint64(9)
	var keyBytes []byte = make([]byte, 0)
	var valueBytes []byte = make([]byte, 0)

	//Check if key data fits inside or overflows further
	if blockPointer+keySize > blockSize {
		keyBytes = append(keyBytes, blockData[blockPointer:blockSize]...)
		keySize -= blockSize - blockPointer
		blockPointer = blockSize
	} else {
		keyBytes = append(keyBytes, blockData[blockPointer:blockPointer+keySize]...)
		blockPointer += keySize
		keySize = 0
	}

	if !tombstone {
		//If the value data fits, then it should be type 3
		if blockPointer+valueSize <= blockSize {
			return nil, nil, 0, 0, errors.New("The block is corrupted")
		}

		valueBytes = append(valueBytes, blockData[blockPointer:blockSize]...)
		valueSize -= blockSize - blockPointer
	}

	return keyBytes, valueBytes, keySize, valueSize, nil
}

// Returns key bytes, value bytes
func getKeysType3(block *blockManager.Block, keySize uint64, valueSize uint64, tombstone bool) ([]byte, []byte, error) {
	blockData := block.GetData()
	dataSize := uint64(block.GetSize())
	blockPointer := uint64(9)
	var keyBytes []byte = make([]byte, 0)
	var valueBytes []byte = make([]byte, 0)

	//Check if key data fits inside or overflows further
	if blockPointer+keySize+valueSize > dataSize {
		return nil, nil, errors.New("The block is corrupted")
	} else {
		keyBytes = append(keyBytes, blockData[blockPointer:blockPointer+keySize]...)
		blockPointer += keySize
	}

	if !tombstone {
		valueBytes = append(valueBytes, blockData[blockPointer:blockPointer+valueSize]...)
	}

	return keyBytes, valueBytes, nil
}

// Returns index if element is found in string slice;
// If element is not found and search should continue, return -1;
// If element is not found and search should not continue, return -2
func FindLastSmallerKey(key string, keys []string) int64 {
	for i := int64(0); i < int64(len(keys)); i++ {
		if key == keys[i] {
			return i
		} else if key < keys[i] {
			if i != 0 {
				return i - 1
			} else {
				return -2
			}
		}
	}
	return -1
}

func CreateCompactSSTable(data []byte, lastElementData []byte, summary_sparsity int, index_sparsity int) {
	generation := getGeneration()

	fileName := "data" + string(os.PathSeparator) + "L0" + string(os.PathSeparator) + "usertable-" + strconv.FormatUint(uint64(generation), 10) + "-compact.bin"
	file, err := os.OpenFile(fileName, os.O_CREATE, 0664)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var indexData []byte
	var summaryData []byte
	summaryData = append(summaryData, lastElementData...)

	var keyBinary []byte
	valueBinary := make([]byte, 8)
	valueSizeBinary := []byte{0, 0, 0, 0, 0, 0, 0, 8}
	keySizeBinary := make([]byte, 8)
	var counter int = 0
	var blockCounter uint64 = 0
	var summaryStart uint64 = 0
	var indexStart uint64 = 0
	var footerStart uint64 = 0

	//Creates the data segment while preparing data for the index segment
	for channelResult := range PrepareSSTableBlocks(fileName, data, true, 0) {
		blockManager.WriteBlock(file, channelResult.Block)
		blockCounter += 1
		if !bytes.Equal(keyBinary, channelResult.Key) {
			if counter%index_sparsity == 0 {
				keyBinary = channelResult.Key
				binary.BigEndian.PutUint64(keySizeBinary, uint64(len(keyBinary)))
				binary.BigEndian.PutUint64(valueBinary, uint64(channelResult.Block.GetOffset()))
				indexData = append(indexData, keySizeBinary...)
				indexData = append(indexData, valueSizeBinary...)
				indexData = append(indexData, keyBinary...)
				indexData = append(indexData, valueBinary...)
			}
			counter += 1
		}
	}

	keyBinary = nil
	indexStart = blockCounter
	counter = 0

	//Creates the index segment while preparing data for the summary segment
	for channelResult := range PrepareSSTableBlocks(fileName, indexData, false, blockCounter) {
		blockManager.WriteBlock(file, channelResult.Block)
		blockCounter += 1
		if !bytes.Equal(keyBinary, channelResult.Key) {
			if counter%summary_sparsity == 0 {
				keyBinary = channelResult.Key
				binary.BigEndian.PutUint64(keySizeBinary, uint64(len(keyBinary)))
				binary.BigEndian.PutUint64(valueBinary, uint64(channelResult.Block.GetOffset()))
				summaryData = append(summaryData, keySizeBinary...)
				summaryData = append(summaryData, valueSizeBinary...)
				summaryData = append(summaryData, keyBinary...)
				summaryData = append(summaryData, valueBinary...)
			}
			counter += 1
		}
	}

	summaryStart = blockCounter

	//Creates summary segment
	for channelResult := range PrepareSSTableBlocks(fileName, summaryData, false, blockCounter) {
		blockManager.WriteBlock(file, channelResult.Block)
		blockCounter += 1
	}

	footerStart = blockCounter

	var footerData []byte = make([]byte, 8*3)

	footerData = binary.BigEndian.AppendUint64(footerData, indexStart)
	footerData = binary.BigEndian.AppendUint64(footerData, summaryStart)
	footerData = binary.BigEndian.AppendUint64(footerData, footerStart)

	var blockData []byte = make([]byte, blockSize)
	copy(blockData[9:], footerData)
	blockData[4] = 0
	binary.BigEndian.PutUint32(blockData[0:4], crc32.ChecksumIEEE(blockData[4:]))
	binary.BigEndian.PutUint32(blockData[5:9], uint32(len(footerData)))

	block := blockManager.InitBlock(fileName, blockCounter, blockData)
	blockManager.WriteBlock(file, block)
}

func findCompact(filePath string, key string) []byte {
	if filePath[len(filePath)-11:] != "compact.bin" {
		panic("Error: findCompact only works on compact sstables")
	}

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		panic("Error: file not found: " + filePath)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		panic("Error: can't get file info")
	}

	var indexStart uint64
	var dataStart uint64
	var footerStart uint64

	footerBlock := blockManager.ReadBlock(file, uint64(fileInfo.Size())/blockSize-1)
	footerData := footerBlock.GetData()
	indexStart = binary.BigEndian.Uint64(footerData[9:17])
	dataStart = binary.BigEndian.Uint64(footerData[17:25])
	footerStart = binary.BigEndian.Uint64(footerData[25:33])

	var firstEntry bool = true
	var blockOffset uint64 = 0

	var dataBlockCheck bool = false
	var keys []string
	var values [][]byte
	var currentSection byte = 0

	var valueBytes []byte
	var keyBytes []byte
	var valueSizeLeft uint64
	var keySizeLeft uint64
	var tombstone bool
	var lastValue []byte = nil

	for {
		if currentSection == 0 && blockOffset >= indexStart {
			break
		} else if currentSection == 1 && blockOffset >= dataStart {
			break
		} else if currentSection == 2 {
			dataBlockCheck = true
			if blockOffset >= footerStart {
				break
			}
		}
		block := blockManager.ReadBlock(file, blockOffset)
		if block == nil {
			break
		} else if block.GetType() == 0 {
			keys, values, err = getKeysType0(block, dataBlockCheck)
			if err != nil {
				panic("Error reading block")
			}

			if firstEntry {
				firstEntry = false
				if key > keys[0] {
					return nil
				}
				keys = keys[1:]
				values = values[1:]
			}

			index := FindLastSmallerKey(key, keys)
			if index == -1 {
				blockOffset += 1
				continue
			} else if index == -2 {
				if dataBlockCheck {
					return nil
				}
				if lastValue == nil {
					return nil
				}
				blockOffset = binary.BigEndian.Uint64(lastValue)
				currentSection += 1
				lastValue = nil
				continue
			}

			blockOffset = binary.BigEndian.Uint64(values[index])
			currentSection += 1
			lastValue = nil
			continue

		} else if block.GetType() == 1 {
			keyBytes, valueBytes, keySizeLeft, valueSizeLeft, err = getKeysType1(block, dataBlockCheck)
			if err != nil {
				panic(err)
			}

			if len(valueBytes) == 0 {
				tombstone = true
			} else {
				tombstone = false
			}
		} else if block.GetType() == 2 {
			keyBytesToAppend, valueBytesToAppend, keySizeLeftNew, valueSizeLeftNew, err := getKeysType2(block, keySizeLeft, valueSizeLeft, tombstone)
			if err != nil {
				panic(err)
			}

			keyBytes = append(keyBytes, keyBytesToAppend...)
			valueBytes = append(valueBytes, valueBytesToAppend...)
			keySizeLeft = keySizeLeftNew
			valueSizeLeft = valueSizeLeftNew
		} else if block.GetType() == 3 {
			keyBytesToAppend, valueBytesToAppend, err := getKeysType3(block, keySizeLeft, valueSizeLeft, tombstone)
			if err != nil {
				panic(err)
			}

			keyBytes = append(keyBytes, keyBytesToAppend...)
			valueBytes = append(valueBytes, valueBytesToAppend...)
			keyConstruct := string(keyBytes)

			if firstEntry {
				firstEntry = false
				if key > keyConstruct {
					return nil
				}
			}

			if keyConstruct == key {
				if dataBlockCheck {
					return valueBytes
				}
				blockOffset = binary.BigEndian.Uint64(valueBytes)
				currentSection += 1

				lastValue = nil
				keyBytes = keyBytes[:0]
				valueBytes = valueBytes[:0]
				keySizeLeft = 0
				valueSizeLeft = 0
				tombstone = false
				continue
			} else if key < keyConstruct {
				if dataBlockCheck {
					return nil
				}
				if lastValue == nil {
					return nil
				}
				blockOffset = binary.BigEndian.Uint64(lastValue)
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

	return nil
}
