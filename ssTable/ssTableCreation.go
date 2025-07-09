package ssTable

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"

	"github.com/Vujovic0/NASP2024/blockManager"
	"github.com/Vujovic0/NASP2024/config"
)

const (
	BlockTypeFull   = 0
	BlockTypeStart  = 1
	BlockTypeMiddle = 2
	BlockTypeEnd    = 3
)

var blockSize uint64 = uint64(config.GlobalBlockSize)

type channelResult struct {
	Block *blockManager.Block
	Key   []byte
}

// Creates blocks for the SSTable without writing them. Works as a generator function when used with keyword "range".
// Takes a byte array formed by entries with certain formatting, file path,
// check if the blocks are data blocks and the starting offset of a block in a file
// If the blocks are data blocks, then 13 bytes are skipped when reading keysize and valuesize
// | CRC 4B | TimeStamp 8B | Tombstone 1B | Keysize 8B | Valuesize 8B | Key... | Value... |
// If entry has tombstone 1, the entry MUST NOT have valueSize and value
// Returns block per block as well as the first key in the block in bytes
//
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
					ch <- &channelResult{Block: block, Key: key}
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

func GetGeneration(increment bool) uint64 {
	var generation uint64
	dataPath := getDataPath()
	filePathMeta := filepath.Join(dataPath, "metaData.bin")
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

// Entires of summary and index are of fomrat |KEYSIZE|VALUESIZE|KEY|VALUE|
// First entry of summary is last entry of data format:
// |KEYSIZE|KEY| and it's used to check if element is out of bounds
// Entires of data are |CRC|TIMESTAMP|TOMBSTONE|KEYSIZE|VALUESIZE|KEY|VALUE
func CreateCompactSSTable(data []byte, lastElementData []byte, summary_sparsity int, index_sparsity int) {
	dataPath := getDataPath()
	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		err := os.Mkdir(dataPath, 0755)
		if err != nil {
			panic(err)
		}
		err = os.Mkdir(dataPath+string(os.PathSeparator)+"L0", 0755)
		if err != nil {
			panic(err)
		}
	}

	generation := getGeneration(true)

	fileName := dataPath + string(os.PathSeparator) + "L0" + string(os.PathSeparator) + "usertable-" + strconv.FormatUint(uint64(generation), 10) + "-compact.bin"
	file, err := os.OpenFile(fileName, os.O_CREATE, 0664)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var indexData []byte
	var summaryData []byte

	var keyBinary []byte

	var counter int = 0
	var blockCounter uint64 = 0
	var summaryStart uint64 = 0
	var indexStart uint64 = 0
	var footerStart uint64 = 0

	//Creates the data segment while preparing data for the index segment
	for channelResult := range PrepareSSTableBlocks(fileName, data, true, 0, false) {
		blockManager.WriteBlock(file, channelResult.Block)
		blockCounter += 1
		if !bytes.Equal(keyBinary, channelResult.Key) {
			keyBinary = channelResult.Key
			if counter%index_sparsity == 0 {
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
		}
		counter += 1
	}

	keyBinary = nil
	indexStart = blockCounter
	counter = 0

	//Creates the index segment while preparing data for the summary segment
	for channelResult := range PrepareSSTableBlocks(fileName, indexData, false, blockCounter, false) {
		blockManager.WriteBlock(file, channelResult.Block)
		blockCounter += 1
		if !bytes.Equal(keyBinary, channelResult.Key) {
			keyBinary = channelResult.Key
			if counter%summary_sparsity == 0 {
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
		}
		counter += 1
	}

	summaryStart = blockCounter

	//Creates summary segment
	for channelResult := range PrepareSSTableBlocks(fileName, summaryData, false, blockCounter, false) {
		blockManager.WriteBlock(file, channelResult.Block)
		blockCounter += 1
	}

	boundStart := blockCounter

	for channelResult := range PrepareSSTableBlocks(fileName, lastElementData, false, blockCounter, true) {
		blockManager.WriteBlock(file, channelResult.Block)
		blockCounter += 1
	}

	footerStart = blockCounter

	var footerData []byte = make([]byte, 0)

	footerData = binary.LittleEndian.AppendUint64(footerData, footerStart)
	footerData = binary.LittleEndian.AppendUint64(footerData, indexStart)
	footerData = binary.LittleEndian.AppendUint64(footerData, summaryStart)
	footerData = binary.LittleEndian.AppendUint64(footerData, boundStart)

	var blockData []byte = make([]byte, 9+len(footerData))
	copy(blockData[9:], footerData)
	blockData[4] = 0
	binary.LittleEndian.PutUint32(blockData[0:4], crc32.ChecksumIEEE(blockData[4:]))
	binary.LittleEndian.PutUint32(blockData[5:9], uint32(len(footerData)))

	block := blockManager.InitBlock(fileName, blockCounter, blockData)
	blockManager.WriteBlock(file, block)
}

// Entires of summary and index are of fomrat |KEYSIZE|VALUESIZE|KEY|VALUE|
// First entry of summary is |8B KEYSIZE|KEY...| and it's used to check if element is out of bounds
// Entires of data are |CRC|TIMESTAMP|TOMBSTONE|KEYSIZE|VALUESIZE|KEY|VALUE
func CreateSeparatedSSTable(data []byte, lastElementData []byte, summary_sparsity int, index_sparsity int) {
	dataPath := getDataPath()
	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		err := os.Mkdir(dataPath, 0755)
		if err != nil {
			panic(err)
		}
		err = os.Mkdir(dataPath+string(os.PathSeparator)+"L0", 0755)
		if err != nil {
			panic(err)
		}
	}

	generation := getGeneration(true)

	fileName := dataPath + string(os.PathSeparator) + "L0" + string(os.PathSeparator) + "usertable-" + strconv.FormatUint(uint64(generation), 10)
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

	var blockData []byte = make([]byte, 9+len(footerData))
	copy(blockData[9:], footerData)
	blockData[4] = 0
	binary.LittleEndian.PutUint32(blockData[0:4], crc32.ChecksumIEEE(blockData[4:]))
	binary.LittleEndian.PutUint32(blockData[5:9], uint32(len(footerData)))

	block := blockManager.InitBlock(fileName, counter, blockData)
	blockManager.WriteBlock(fileSummary, block)
}
