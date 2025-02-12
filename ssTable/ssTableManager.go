package ssTable

import (
	"NASP2024/blockManager"
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
)

var blockSize = 1024 * 4

type channelResult struct {
	Block *blockManager.Block
	Key   []byte
}

// Writes data serialized using big endian. Can work as a generator function when used with keyword "range".
// Takes a byte array formed by entries with certain formatting
// | KEYOFFSET | Keysize 8B | Valuesize 8B | Key... | Value... |
func PrepareBlocks(filePath string, data []byte, keySizeOffset int) <-chan *channelResult {
	ch := make(chan *channelResult)

	process := func() {
		if len(data) < keySizeOffset+16 {
			panic("Invalid data")
		}

		var dataPointer uint64 = 0
		blockPointer := 9
		blockData := make([]byte, blockSize)
		blockOffset := uint64(0)
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
				entryValueSize = binary.BigEndian.Uint64(data[dataPointer+uint64(keySizeOffset)+8 : dataPointer+uint64(keySizeOffset)+16])
				entrySize = entryKeySize + entryValueSize + uint64(keySizeOffset) + 16
				entrySizeLeft = entrySize
				if newBlockCheck {
					key = data[dataPointer+uint64(keySizeOffset)+16 : dataPointer+uint64(keySizeOffset)+16+entryKeySize]
					newBlockCheck = false
				}
			}

			if uint64(blockSize-blockPointer) >= entrySize {
				copy(blockData[blockPointer:], data[dataPointer:uint64(dataPointer)+entrySize])
				dataPointer += entrySize
				blockPointer += int(entrySize)
				if dataPointer == uint64(len(data)) {
					blockType = 0

					blockData[4] = blockType
					binary.BigEndian.PutUint32(blockData[5:9], uint32(blockPointer-9))

					crcValue = crc32.ChecksumIEEE(blockData[4:])
					binary.BigEndian.PutUint32(blockData[0:4], crcValue)

					block = blockManager.InitBlock(filePath, blockOffset, blockType, int(blockPointer-9), blockData)
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

				block = blockManager.InitBlock(filePath, blockOffset, blockType, blockPointer-9, blockData)
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

					block = blockManager.InitBlock(filePath, blockOffset, blockType, int(entrySizeLeft), blockData)
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
					block = blockManager.InitBlock(filePath, blockOffset, blockType, blockSize-9, blockData)
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

// TODO: Figure out how to make a file that is a generation bigger than the last one
func CreateSSTable(filePath string, data []byte, lastElementData []byte, summary_sparsity int, index_sparsity int) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0664)
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

	for channelResult := range PrepareBlocks(filePath, data, 21) {
		blockManager.WriteBlock(file, channelResult.Block)
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

	for channelResult := range PrepareBlocks(filePath, indexData, 0) {
		blockManager.WriteBlock(file, channelResult.Block)
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
}
