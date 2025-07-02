package ssTable

import (
	"NASP2024/blockManager"
	"NASP2024/config"
	"NASP2024/utils"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	BlockTypeFull   = 0
	BlockTypeStart  = 1
	BlockTypeMiddle = 2
	BlockTypeEnd    = 3
)

var blockSize uint64 = uint64(config.GlobalBlockSize)

type channelResult struct {
	Block  *blockManager.Block
	Key    []byte
	Offset int64
}

// Creates blocks for the SSTable without writing them. Works as a generator function when used with keyword "range".
// Takes a byte array formed by entries with certain formatting, file path,
// check if the blocks are data blocks and the starting offset of a block in a file
// If the blocks are data blocks, then 13 bytes are skipped when reading keysize and valuesize
// | CRC 4B | TimeStamp 8B | Tombstone 1B | Keysize 8B | Valuesize 8B | Key... | Value... |
// If entry has tombstone 1, the entry MUST NOT have valueSize and value
// Returns block per block as well as the first key in the block in bytes

// boze me sacuvaj, prljavog li koda
func PrepareSSTableBlocks(filePath string, data []byte, dataBlocksCheck bool, blockOffset uint64, firstSummaryBlock bool) <-chan *channelResult {
	ch := make(chan *channelResult)

	process := func() {

		// if dataBlocksCheck {
		// 	if len(data) < keySizeOffset+16 {
		// 		panic("Invalid data")
		// 	}
		// }

		var dataPointer uint64 = 0
		//skips the header of the block |CRC 4B|Type 1B|Data size 4B|
		blockHeader := uint64(9)
		blockPointer := blockHeader
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
		var entryHeaderSize int

		for dataPointer < uint64(len(data)) {

			//checks how many bytes header takes up, and what type of entry is being read
			//aka if it's a data entry, other type of entry or boundary entry
			if newEntryCheck {
				header := InitHeader(data[dataPointer:], dataBlocksCheck)
				entryKeySize = GetKeySize(dataPointer, data, header)
				//dataPointer+12 represents the tombstone byte only in data segments, not index or summary
				if dataBlocksCheck {
					tombstone := GetTombstone(dataPointer, data, header)
					if tombstone {
						header.valueSizeBytes = 0
						entryValueSize = 0
						entryHeaderSize = GetHeaderSize(header)
					} else {
						entryValueSize = GetValueSize(dataPointer, data, header, firstSummaryBlock, dataBlocksCheck)
						entryHeaderSize = GetHeaderSize(header)
					}
				} else {
					//checks if the block represents the boundary of the entire sstable
					//if so, the value is left out because the boundary is only the key
					if firstSummaryBlock {
						entryValueSize = 0
						firstSummaryBlock = false
					} else {
						entryValueSize = GetValueSize(dataPointer, data, header, firstSummaryBlock, dataBlocksCheck)
					}
					entryHeaderSize = GetHeaderSize(header)
				}
				//calculates how many bytes the entry should take up
				entrySize = entryKeySize + entryValueSize + uint64(entryHeaderSize)
				entrySizeLeft = entrySize
				//if this is the first block aka type 0 or type 1 the key is saved for easier use
				//during creating of index and summary
				if newBlockCheck {
					key = data[dataPointer+uint64(entryHeaderSize) : dataPointer+uint64(entryHeaderSize)+entryKeySize]
					newBlockCheck = false
				}
			}

			//checks if entry can fit into the empty space of the block
			if blockSize-blockPointer >= entrySize {
				copy(blockData[blockPointer:], data[dataPointer:uint64(dataPointer)+entrySize])
				dataPointer += entrySize
				blockPointer += entrySize
				if dataPointer == uint64(len(data)) {
					blockType = 0

					blockData[4] = blockType
					binary.LittleEndian.PutUint32(blockData[5:9], uint32(blockPointer-9))

					crcValue = crc32.ChecksumIEEE(blockData[4:])
					binary.LittleEndian.PutUint32(blockData[0:4], crcValue)

					block = blockManager.InitBlock(filePath, blockOffset, blockData)
					ch <- &channelResult{Block: block, Key: key, Offset: int64(blockOffset)}
					break
				}
				continue
			} else if blockPointer != blockHeader {
				blockType = 0

				blockData[4] = blockType
				binary.LittleEndian.PutUint32(blockData[5:9], uint32(blockPointer-blockHeader))

				crcValue = crc32.ChecksumIEEE(blockData[4:])
				binary.LittleEndian.PutUint32(blockData[0:4], crcValue)

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
					binary.LittleEndian.PutUint32(blockData[5:9], uint32(entrySizeLeft))

					crcValue = crc32.ChecksumIEEE(blockData[4:])
					binary.LittleEndian.PutUint32(blockData[0:4], crcValue)

					block = blockManager.InitBlock(filePath, blockOffset, blockData)
					newEntryCheck = true
					dataPointer += entrySizeLeft
				} else {
					entrySizeLeft -= uint64(blockSize - blockHeader)

					binary.LittleEndian.PutUint32(blockData[5:9], uint32(blockSize-9))
					blockData[4] = blockType
					copy(blockData[9:], data[dataPointer:uint64(dataPointer)+uint64(blockSize-9)])

					crcValue = crc32.ChecksumIEEE(blockData[4:])
					binary.LittleEndian.PutUint32(blockData[0:4], crcValue)

					dataPointer += uint64(blockSize - 9)
					block = blockManager.InitBlock(filePath, blockOffset, blockData)
				}
			}
			blockData = make([]byte, blockSize)
			blockPointer = blockHeader
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

func getGeneration(increment bool) uint64 {
	var generation uint64

	filePathMeta := filepath.Join("data", "metaData.bin")
	_, err := os.Stat(filePathMeta)
	fileExists := err == nil || !os.IsNotExist(err)

	metaFile, err := os.OpenFile(filePathMeta, os.O_RDWR|os.O_CREATE, 0664)
	if err != nil {
		panic(err)
	}
	defer metaFile.Close()
	var block *blockManager.Block
	if fileExists {
		block = blockManager.ReadBlock(metaFile, 0)
		generation = binary.LittleEndian.Uint64(block.GetData()[9:17])
	} else {
		generation = 0
	}
	if increment {
		generation += 1
		dataPlaceholder := make([]byte, blockSize)
		binary.LittleEndian.PutUint32(dataPlaceholder[5:9], 8)
		binary.LittleEndian.PutUint64(dataPlaceholder[9:17], generation)
		crcValue := crc32.ChecksumIEEE(dataPlaceholder[4:])
		binary.LittleEndian.PutUint32(dataPlaceholder[:4], crcValue)
		block = blockManager.InitBlock(filePathMeta, 0, dataPlaceholder)
		blockManager.WriteBlock(metaFile, block)
	}

	return generation
}

// Returns a slice of keys and their values in bytes of a full block.
// DataBlockCheck is true when reading from a data segment in sstable because entries
// in summary and index don't have CRC, timestamp and tombstone
// If value of an entry is an empty slice, it means that the tombstone is set to 1
func getKeysType0(block *blockManager.Block, dataBlockCheck bool, summaryBound uint64) ([][]byte, [][]byte, error) {
	firstSummaryBlock := false
	if summaryBound != 0 {
		if block.GetOffset() == summaryBound {
			firstSummaryBlock = true
		}
	}
	blockData := block.GetData()
	dataSize := uint64(block.GetSize())
	dataSize += 9

	var tombstone bool = false

	blockPointer := uint64(9)
	var keySlice [][]byte
	var valueSlice [][]byte
	var valueSize uint64

	for blockPointer < dataSize {
		header := InitHeader(blockData[blockPointer:], dataBlockCheck)

		if blockPointer+uint64(GetHeaderSize(header)) > dataSize {
			return nil, nil, errors.New("the block is corrupted: header bigger than data")
		}

		if dataBlockCheck {
			tombstone = GetTombstone(blockPointer, blockData, header)
		}

		keySize := GetKeySize(blockPointer, blockData, header)

		if tombstone {
			valueSize = 0
		} else {
			valueSize = GetValueSize(blockPointer, blockData, header, firstSummaryBlock, dataBlockCheck)
		}

		keyStart := blockPointer + uint64(GetHeaderSize(header))
		keyEnd := keyStart + keySize
		keyBytes := blockData[keyStart:keyEnd]

		valueBytes := make([]byte, 0)
		valueStart := keyEnd
		valueEnd := valueStart + valueSize
		if !firstSummaryBlock {
			valueBytes = blockData[valueStart:valueEnd]
		}

		keySlice = append(keySlice, keyBytes)
		valueSlice = append(valueSlice, valueBytes)
		tombstone = false
		firstSummaryBlock = false

		blockPointer += uint64(GetHeaderSize(header)) + keySize + valueSize
	}

	return keySlice, valueSlice, nil
}

// Returns key bytes, value bytes, key size left, value size left
func getKeysType1(block *blockManager.Block, dataBlockCheck bool, summaryBound uint64) ([]byte, []byte, uint64, uint64, error) {
	firstSummaryBlock := false
	if summaryBound != 0 {
		if block.GetOffset() == summaryBound {
			firstSummaryBlock = true
		}
	}
	blockData := block.GetData()
	dataSize := uint64(block.GetSize())
	dataSize += 9

	blockPointer := uint64(9)
	var keyBytes []byte = make([]byte, 0)
	var valueBytes []byte = make([]byte, 0)
	var valueSize uint64 = 0
	var tombstone bool = false

	header := InitHeader(blockData[9:], dataBlockCheck)
	if blockPointer+uint64(GetHeaderSize(header)) > dataSize {
		return nil, nil, 0, 0, errors.New("the block is corrupted: header bigger than data")
	}
	if dataBlockCheck {
		tombstone = GetTombstone(blockPointer, blockData, header)
		valueSize = GetValueSize(blockPointer, blockData, header, firstSummaryBlock, dataBlockCheck)
	}

	keySize := GetKeySize(blockPointer, blockData, header)

	headerSize := uint64(GetHeaderSize(header))
	//Check if key data fits inside or overflows further
	if blockPointer+headerSize+keySize > blockSize {
		keyBytes = append(keyBytes, blockData[blockPointer+headerSize:blockSize]...)
		keySize = keySize - (blockSize - blockPointer - headerSize)
		blockPointer = blockSize
	} else {
		keyBytes = append(keyBytes, blockData[blockPointer+headerSize:blockPointer+headerSize+keySize]...)
		blockPointer = blockPointer + headerSize + keySize
		keySize = 0
	}

	if !tombstone {
		//If the value data fits, then it should be type 0
		if dataBlockCheck && blockPointer+valueSize <= blockSize {
			return nil, nil, 0, 0, errors.New("the block is corrupted")
		}
		if !firstSummaryBlock {
			valueBytes = append(valueBytes, blockData[blockPointer:blockSize]...)
			if dataBlockCheck && !tombstone {
				valueSize = valueSize - (blockSize - blockPointer)
			}
		}

	}

	return keyBytes, valueBytes, keySize, valueSize, nil
}

// Returns key bytes, value bytes, key size left, value size left
func getKeysType2(block *blockManager.Block, keySize uint64, valueSize uint64, tombstone bool, dataBlock bool) ([]byte, []byte, uint64, uint64, error) {
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
		if !config.VariableEncoding && blockPointer+valueSize <= blockSize {
			return nil, nil, 0, 0, errors.New("the block is corrupted")
		}

		valueBytes = append(valueBytes, blockData[blockPointer:blockSize]...)
		if dataBlock && !tombstone {
			valueSize = valueSize - (blockSize - blockPointer)
		}
	}

	return keyBytes, valueBytes, keySize, valueSize, nil
}

// Returns key bytes, value bytes
func getKeysType3(block *blockManager.Block, keySize uint64, valueSize uint64, tombstone bool, dataBlock bool) ([]byte, []byte, error) {
	blockData := block.GetData()
	dataSize := uint64(block.GetSize())
	dataSize += 9
	blockPointer := uint64(9)
	var keyBytes []byte = make([]byte, 0)
	var valueBytes []byte = make([]byte, 0)

	//Check if key data fits inside or overflows further
	if !config.VariableEncoding && keySize+valueSize > dataSize {
		return nil, nil, errors.New("the block is corrupted")
	} else {
		keyBytes = append(keyBytes, blockData[blockPointer:blockPointer+keySize]...)
		blockPointer += keySize
	}

	if !tombstone {
		if dataBlock {
			valueBytes = append(valueBytes, blockData[blockPointer:blockPointer+valueSize]...)
		} else {
			valueBytes = append(valueBytes, blockData[blockPointer:]...)
		}
	}

	return keyBytes, valueBytes, nil
}

// Returns index if element is found in string slice;
// If element is not found and search should continue return -1;
// If element is not found and search should not continue return -2
func FindLastSmallerKey(key []byte, keys [][]byte) int64 {
	for i := int64(0); i < int64(len(keys)); i++ {
		compareResult := bytes.Compare(key, keys[i])
		if compareResult == 0 {
			return i
		} else if compareResult == -1 {
			if i != 0 {
				return i - 1
			} else {
				return -2
			}
		}
	}
	return -1
}

func ParseEntries(data []byte) map[string]string {
	m := make(map[string]string)
	buf := bytes.NewBuffer(data)
	for buf.Len() > 0 {
		keyLen, _ := binary.ReadUvarint(buf)
		key := make([]byte, keyLen)
		buf.Read(key)
		valLen, _ := binary.ReadUvarint(buf)
		val := make([]byte, valLen)
		buf.Read(val)
		m[string(key)] = string(val)
	}
	return m
}

func BuildDictionary(m map[string]string) *Dictionary {
	dict := NewDictionary()
	for _, v := range m {
		dict.Encode(v)
	}
	return dict
}

// Entires of summary and index are of fomrat |KEYSIZE|VALUESIZE|KEY|VALUE|
// First entry of summary is last entry of data format:
// |KEYSIZE|KEY| and it's used to check if element is out of bounds
// Entires of data are |CRC|TIMESTAMP|TOMBSTONE|KEYSIZE|VALUESIZE|KEY|VALUE
func CreateCompactSSTable(data []byte, lastElementData []byte, summarySparsity int, indexSparsity int) {
	os.MkdirAll("data/L0", 0755)
	generation := getGeneration(true)
	fileName := fmt.Sprintf("data/L0/usertable-%d-compact.bin", generation)
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var (
		indexData    []byte
		summaryData  []byte
		keyBinary    []byte
		counter      int
		blockCounter uint64
		summaryStart uint64
		indexStart   uint64
		footerStart  uint64
	)

	// Write data blocks and build index
	for res := range PrepareSSTableBlocks(fileName, data, true, 0, false) {
		blockManager.WriteBlock(file, res.Block)
		blockCounter++
		if !bytes.Equal(keyBinary, res.Key) {
			keyBinary = res.Key
			if counter%indexSparsity == 0 {
				offset := res.Offset
				indexData = appendIndexEntry(indexData, keyBinary, offset)
			}
		}
		counter++
	}

	// Write index blocks and build summary
	keyBinary = nil
	indexStart = blockCounter
	counter = 0

	for res := range PrepareSSTableBlocks(fileName, indexData, false, blockCounter, false) {
		blockManager.WriteBlock(file, res.Block)
		blockCounter++
		if !bytes.Equal(keyBinary, res.Key) {
			keyBinary = res.Key
			if counter%summarySparsity == 0 {
				offset := res.Offset
				summaryData = appendIndexEntry(summaryData, keyBinary, offset)
			}
		}
		counter++
	}

	// Write summary blocks
	summaryStart = blockCounter
	for res := range PrepareSSTableBlocks(fileName, summaryData, false, blockCounter, false) {
		blockManager.WriteBlock(file, res.Block)
		blockCounter++
	}

	// Write final boundary key block
	boundStart := blockCounter
	for res := range PrepareSSTableBlocks(fileName, lastElementData, false, blockCounter, true) {
		blockManager.WriteBlock(file, res.Block)
		blockCounter++
	}

	// Write footer block
	footerStart = blockCounter
	footerData := encodeFooter(footerStart, indexStart, summaryStart, boundStart)
	footerBlock := blockManager.InitBlock(fileName, blockCounter, footerData)
	blockManager.WriteBlock(file, footerBlock)

	// Build and save dictionary
	entries := ParseEntries(data)
	values := make([]string, 0, len(entries))
	for _, v := range entries {
		values = append(values, string(v))
	}
	_, finalDict := CompressWithDictionary(values)
	dictPath := fmt.Sprintf("data/usertable-%d.dict", generation)
	dict, _ := LoadDictionaryFromFile(dictPath)
	if err != nil {
		panic("Could not load dictionary: " + err.Error())
	}

	serialized := make([][]byte, 0, len(entries))
	for _, raw := range entries {
		strVal := string(raw)
		decompressed := DecompressSingle(strVal, dict)
		elem := utils.ParseElementFromString(decompressed) // You'll need a helper for this
		serialized = append(serialized, utils.SerializeEntry(&elem, true))
	}

	SaveDictionaryToFile(finalDict, dictPath)
}

func appendIndexEntry(buf []byte, key []byte, offset int64) []byte {
	if !config.VariableEncoding {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(len(key)))
		buf = append(buf, key...)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(offset))
	} else {
		buf = binary.AppendUvarint(buf, uint64(len(key)))
		buf = append(buf, key...)
		buf = binary.AppendUvarint(buf, uint64(offset))
	}
	return buf
}

func encodeFooter(footerStart, indexStart, summaryStart, boundStart uint64) []byte {
	footer := make([]byte, 0)
	footer = binary.LittleEndian.AppendUint64(footer, footerStart)
	footer = binary.LittleEndian.AppendUint64(footer, indexStart)
	footer = binary.LittleEndian.AppendUint64(footer, summaryStart)
	footer = binary.LittleEndian.AppendUint64(footer, boundStart)

	block := make([]byte, config.BlockSize)
	copy(block[9:], footer)
	block[4] = 0
	binary.LittleEndian.PutUint32(block[0:4], crc32.ChecksumIEEE(block[4:]))
	binary.LittleEndian.PutUint32(block[5:9], uint32(len(footer)))
	return block
}

// Entires of summary and index are of fomrat |KEYSIZE|VALUESIZE|KEY|VALUE|
// First entry of summary is |8B KEYSIZE|KEY...| and it's used to check if element is out of bounds
// Entires of data are |CRC|TIMESTAMP|TOMBSTONE|KEYSIZE|VALUESIZE|KEY|VALUE
func CreateSeparatedSSTable(data []byte, lastElementData []byte, summary_sparsity int, index_sparsity int) {
	if _, err := os.Stat("data"); os.IsNotExist(err) {
		err := os.Mkdir("data", 0644)
		if err != nil {
			panic(err)
		}
		err = os.Mkdir("data"+string(os.PathSeparator)+"L0", 0644)
		if err != nil {
			panic(err)
		}
	}

	generation := getGeneration(true)

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

	var keyBinary []byte

	var counter uint64 = 0

	//Creates the data segment while preparing data for the index segment
	for channelResult := range PrepareSSTableBlocks(FILEDATAPATH, data, true, 0, false) {
		blockManager.WriteBlock(fileData, channelResult.Block)
		if !bytes.Equal(keyBinary, channelResult.Key) {
			keyBinary = channelResult.Key
			if counter%uint64(index_sparsity) == 0 {
				if !config.VariableEncoding {
					indexData = binary.LittleEndian.AppendUint64(indexData, uint64(len(keyBinary)))
					indexData = append(indexData, keyBinary...)
					indexData = binary.LittleEndian.AppendUint64(indexData, uint64(channelResult.Block.GetOffset()))
				} else {
					indexData = binary.AppendUvarint(indexData, uint64(len(keyBinary)))
					indexData = append(indexData, keyBinary...)
					indexData = binary.AppendUvarint(indexData, uint64(channelResult.Block.GetOffset()))
				}
			}
			counter += 1
		}
	}

	keyBinary = nil
	counter = 0

	//Creates the index segment while preparing data for the summary segment
	for channelResult := range PrepareSSTableBlocks(FILEINDEXPATH, indexData, false, 0, false) {
		blockManager.WriteBlock(fileIndex, channelResult.Block)
		if !bytes.Equal(keyBinary, channelResult.Key) {
			keyBinary = channelResult.Key
			if counter%uint64(summary_sparsity) == 0 {
				if !config.VariableEncoding {
					summaryData = binary.LittleEndian.AppendUint64(summaryData, uint64(len(keyBinary)))
					summaryData = append(summaryData, keyBinary...)
					summaryData = binary.LittleEndian.AppendUint64(summaryData, uint64(channelResult.Block.GetOffset()))
				} else {
					summaryData = binary.AppendUvarint(summaryData, uint64(len(keyBinary)))
					summaryData = append(summaryData, keyBinary...)
					summaryData = binary.AppendUvarint(summaryData, uint64(channelResult.Block.GetOffset()))
				}
			}
			counter += 1
		}
	}

	counter = 0
	//Creates summary segment
	for channelResult := range PrepareSSTableBlocks(FILESUMMARYPATH, summaryData, false, 0, false) {
		blockManager.WriteBlock(fileSummary, channelResult.Block)
		counter += 1
	}

	boundStart := counter

	for channelResult := range PrepareSSTableBlocks(FILESUMMARYPATH, lastElementData, false, uint64(counter), true) {
		blockManager.WriteBlock(fileSummary, channelResult.Block)
		counter += 1
	}

	footerStart := counter

	var footerData []byte = make([]byte, 0)

	footerData = binary.LittleEndian.AppendUint64(footerData, uint64(footerStart))
	footerData = binary.LittleEndian.AppendUint64(footerData, uint64(boundStart))

	var blockData []byte = make([]byte, config.GlobalBlockSize)
	copy(blockData[9:], footerData)
	blockData[4] = 0
	binary.LittleEndian.PutUint32(blockData[0:4], crc32.ChecksumIEEE(blockData[4:]))
	binary.LittleEndian.PutUint32(blockData[5:9], uint32(len(footerData)))

	block := blockManager.InitBlock(fileName, counter, blockData)
	blockManager.WriteBlock(fileSummary, block)
}

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

	footerBlock := blockManager.ReadBlock(file, uint64(fileInfo.Size())/blockSize-1)
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

			index := FindLastSmallerKey(key, keys)
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

			index := FindLastSmallerKey(key, keys)
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

// | CRC 4B | TimeStamp 8B | Tombstone 1B | Keysize 8B | Valuesize 8B | Key... | Value... |
// KeyOnlyCheck will assure that only the key with its size is serialized
func SerializeEntryHelper(key string, value string, tombstone bool, keyOnly bool) []byte {
	if !config.VariableEncoding {
		keyBytes := []byte(key)
		keySize := len(keyBytes)
		var dataBytes []byte
		if keyOnly {
			dataBytes = make([]byte, 8+keySize)
			binary.LittleEndian.PutUint64(dataBytes[0:8], uint64(keySize))
			copy(dataBytes[8:], keyBytes)
			return dataBytes
		}
		valueBytes := []byte(value)
		valueSize := len(valueBytes)
		timestamp := time.Now().Unix()
		sizeReserve := 4 + 8 + 1 + 8 + 8 + keySize + valueSize
		if tombstone {
			sizeReserve -= valueSize + 8
		}
		dataBytes = make([]byte, sizeReserve)
		binary.LittleEndian.PutUint64(dataBytes[4:12], uint64(timestamp))
		if tombstone {
			dataBytes[12] = 1
		}
		binary.LittleEndian.PutUint64(dataBytes[13:21], uint64(keySize))
		if !tombstone {
			binary.LittleEndian.PutUint64(dataBytes[21:29], uint64(valueSize))
			copy(dataBytes[29+keySize:], valueBytes)
			copy(dataBytes[29:29+keySize], keyBytes)
		} else {
			copy(dataBytes[21:21+keySize], keyBytes)
		}
		crc := crc32.ChecksumIEEE(dataBytes[4:])
		binary.LittleEndian.PutUint32(dataBytes[:4], crc)
		return dataBytes
	} else {
		data := make([]byte, 0)
		if keyOnly {
			keyBytes := []byte(key)
			data = binary.AppendUvarint(data, uint64(len(keyBytes)))
			data = append(data, keyBytes...)
			return data
		}
		//make space for crc32
		data = append(data, []byte{0, 0, 0, 0}...)
		timestamp := time.Now().Unix()
		keyBytes := []byte(key)
		valueBytes := []byte(value)
		//append timestamp data
		data = binary.AppendUvarint(data, uint64(timestamp))
		//append tombstone
		if tombstone {
			data = binary.AppendUvarint(data, 1)
		} else {
			data = binary.AppendUvarint(data, 0)
		}
		//append key size
		data = binary.AppendUvarint(data, uint64(len(keyBytes)))
		//apend value size
		if !tombstone {
			data = binary.AppendUvarint(data, uint64(len(valueBytes)))
		}
		//append key
		data = append(data, keyBytes...)
		//append value
		data = append(data, valueBytes...)

		crc := crc32.ChecksumIEEE(data[4:])
		//fill reserved data with crc32
		binary.LittleEndian.PutUint32(data[0:4], crc)
		return data
	}
}

// Takes key and returns the value associated with the key as a byte slice.
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

	iterator := 1
	for iterator <= int(generation) {
		genStr := strconv.FormatUint(uint64(iterator), 10)
		iterator += 1
		fileName := "usertable-" + genStr + "-compact.bin"
		_, ok := files[fileName]
		if !ok {
			fileName = "usertable-" + genStr + "-summary.bin"
			_, ok = files[fileName]
			if !ok {
				continue
			}
			valueBytes, err = findSeparated(files[fileName], key, 0)
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

		} else {
			valueBytes, err = findCompact(files[fileName], key)
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

func getReadOrder() {

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
	footerBlockIndex := fileSize/int64(config.GlobalBlockSize) - 1

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

type Element struct {
	Key       string
	Value     string
	Timestamp int64
	Tombstone bool
}

func LoadAllElements(path string) []Element {
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	dictPath := strings.Replace(path, ".bin", ".dict", 1)
	dict, _ := LoadDictionaryFromFile(dictPath)

	var elements []Element
	blockOffset := uint64(0)

	for {
		block := blockManager.ReadBlock(file, blockOffset)
		if block == nil {
			break
		}

		blockOffset++
		if block.GetType() != 0 {
			continue
		}

		data := block.GetData()[9 : 9+block.GetSize()]
		buf := bytes.NewBuffer(data)
		for buf.Len() > 0 {
			crc := make([]byte, 4)
			buf.Read(crc)

			timestamp, _ := binary.ReadVarint(buf)
			tombstone, _ := binary.ReadUvarint(buf)
			keyLen, _ := binary.ReadUvarint(buf)
			valLen, _ := binary.ReadUvarint(buf)

			key := make([]byte, keyLen)
			buf.Read(key)
			val := make([]byte, valLen)
			buf.Read(val)

			decoded := DecompressSingle(string(val), dict)

			elements = append(elements, Element{
				Key:       string(key),
				Value:     decoded,
				Timestamp: timestamp,
				Tombstone: tombstone == 1,
			})
		}
	}

	return elements
}

func EncodeData(elems []Element) []byte {
	var buf bytes.Buffer
	for _, elem := range elems {
		crc := make([]byte, 4)
		binary.Write(&buf, binary.LittleEndian, crc)
		binary.Write(&buf, binary.BigEndian, elem.Timestamp)
		tomb := uint64(0)
		if elem.Tombstone {
			tomb = 1
		}
		binary.Write(&buf, binary.BigEndian, tomb)
		binary.Write(&buf, binary.BigEndian, uint64(len(elem.Key)))
		binary.Write(&buf, binary.BigEndian, uint64(len(elem.Value)))
		buf.Write([]byte(elem.Key))
		buf.Write([]byte(elem.Value))
	}
	return buf.Bytes()
}

func EncodeLastEntry(elem Element) []byte {
	buf := make([]byte, 0)
	buf = binary.AppendUvarint(buf, uint64(len(elem.Key)))
	buf = append(buf, []byte(elem.Key)...)
	return buf
}

func PromoteLevel(currentLevel int) string {
	nextLevel := currentLevel + 1
	os.MkdirAll(fmt.Sprintf("data/L%d", nextLevel), 0755)
	return fmt.Sprintf("data/L%d", nextLevel)
}

func WriteMergedSSTable(entries []*Element) {
	var values []string
	for _, e := range entries {
		values = append(values, e.Value)
	}

	compressedData, dict := CompressWithDictionary(values)
	split := bytes.Split(compressedData, []byte("|"))
	for i, e := range entries {
		if i < len(split) {
			e.Value = string(split[i])
		}
	}

	var data []byte
	for _, e := range entries {
		entry := initEntry(0, e.Tombstone, uint64(e.Timestamp), []byte(e.Key), []byte(e.Value))
		data = append(data, SerializeEntry(entry, e.Tombstone)...)
	}

	last := entries[len(entries)-1]
	lastKeyBytes := []byte(last.Key)
	lastKeySize := uint64(len(lastKeyBytes))
	var lastBuf []byte
	if config.VariableEncoding {
		lastBuf = binary.AppendUvarint(lastBuf, lastKeySize)
	} else {
		lastBuf = binary.LittleEndian.AppendUint64(lastBuf, lastKeySize)
	}
	lastBuf = append(lastBuf, lastKeyBytes...)

	CreateCompactSSTable(data, lastBuf, 2, 4)

	err := SaveDictionaryToFile(dict, "data/dictionary.dict")
	if err != nil {
		panic("Failed to save dict: " + err.Error())
	}
}

func CleanLevel0() {
	files, _ := filepath.Glob("data/L0/*.bin")
	for _, f := range files {
		os.Remove(f)
	}
}

func ReadDecompressedValues(path string, dictPath string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	dmap, err := LoadDictionaryFromFile(dictPath)
	if err != nil {
		return nil, err
	}

	// Rekonstruišemo dictionary
	dict := NewDictionary()
	for word, code := range dmap {
		parsed, _ := strconv.ParseUint(code, 10, 64)
		dict.EncodeMap[word] = parsed
		dict.DecodeMap[parsed] = word
	}

	return DecompressWithDictionary(data, dict)
}
func Flush(elements []Element, sstFilename string) error {
	var values []string
	for _, el := range elements {
		values = append(values, el.Value)
	}

	compressedData, dict := CompressWithDictionary(values)
	split := bytes.Split(compressedData, []byte("|"))
	if len(split) != len(elements) {
		return errors.New("compression mismatch with element count")
	}
	for i := range elements {
		elements[i].Value = string(split[i])
	}

	var ptrElements []*Element
	for i := range elements {
		ptrElements = append(ptrElements, &elements[i])
	}
	WriteMergedSSTable(ptrElements)

	dictFile := strings.Replace(sstFilename, ".sst", ".dict", 1)
	return SaveDictionaryToFile(dict, dictFile)
}

func ReadSSTable(path string) ([]Element, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var elements []Element
	reader := bytes.NewReader(data)

	for reader.Len() > 0 {
		// Read size of key
		var keySize uint64
		if config.VariableEncoding {
			keySize, err = binary.ReadUvarint(reader)
		} else {
			var fixedSize uint64
			err = binary.Read(reader, binary.LittleEndian, &fixedSize)
			keySize = fixedSize
		}
		if err != nil {
			break
		}

		key := make([]byte, keySize)
		_, err = reader.Read(key)
		if err != nil {
			break
		}

		// Read size of value
		var valSize uint64
		if config.VariableEncoding {
			valSize, err = binary.ReadUvarint(reader)
		} else {
			var fixedValSize uint64
			err = binary.Read(reader, binary.LittleEndian, &fixedValSize)
			valSize = fixedValSize
		}
		if err != nil {
			break
		}

		val := make([]byte, valSize)
		_, err = reader.Read(val)
		if err != nil {
			break
		}

		// Read timestamp and tombstone
		var timestamp uint64
		err = binary.Read(reader, binary.LittleEndian, &timestamp)
		if err != nil {
			break
		}

		tomb := make([]byte, 1)
		_, err = reader.Read(tomb)
		if err != nil {
			break
		}

		elements = append(elements, Element{
			Key:       string(key),
			Value:     string(val),
			Timestamp: int64(timestamp),
			Tombstone: tomb[0] == 1,
		})
	}

	return elements, nil
}

func ReadSSTableWithDecompression(path string, dictPath string) ([]Element, error) {
	entries, err := ReadSSTable(path)
	if err != nil {
		return nil, err
	}

	dictMap, err := LoadDictionaryFromFile(dictPath)
	if err != nil {
		return nil, err
	}

	decoded := make(map[uint64]string)
	for k, v := range dictMap {
		code, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			continue
		}
		decoded[code] = v
	}

	for i := range entries {
		parts := strings.Split(entries[i].Value, "|")
		var val strings.Builder
		for _, p := range parts {
			if len(p) == 0 {
				continue
			}
			idx, _ := binary.Uvarint([]byte(p))
			word := decoded[idx]
			val.WriteString(word)
		}
		entries[i].Value = val.String()
	}

	return entries, nil
}
