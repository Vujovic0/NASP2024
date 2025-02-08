package ssTable

import (
	"NASP2024/blockManager"
	"encoding/binary"
	"hash/crc32"
	"os"
)

var blockSize = 1024 * 4

// Writes data serialized using big endian. Can work as a generator function when used with keyword "range".
func WriteData(filePath string, data []byte) <-chan *blockManager.Block {
	ch := make(chan *blockManager.Block)

	process := func() {
		if len(data) < 37 {
			panic("Invalid data")
		}

		file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		var dataPointer uint64 = 0
		blockPointer := 9
		blockData := make([]byte, blockSize)
		blockOffset := 0
		var crcValue uint32 = 0
		var blockType byte = 0

		var entryValueSize uint64 = 0
		var entryKeySize uint64 = 0
		var entrySize uint64 = 0
		var entrySizeLeft uint64 = 0
		var block *blockManager.Block
		var newEntryCheck bool = true

		for dataPointer < uint64(len(data)) {

			if newEntryCheck {
				entryKeySize = binary.BigEndian.Uint64(data[dataPointer+21 : dataPointer+29])
				entryValueSize = binary.BigEndian.Uint64(data[dataPointer+29 : dataPointer+37])
				entrySize = entryKeySize + entryValueSize + 37
				entrySizeLeft = entrySize
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
					blockManager.WriteBlock(file, block)
					ch <- block
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
			blockManager.WriteBlock(file, block)
			ch <- block
		}
	}

	go func() {
		process()
		close(ch)
	}()

	return ch
}
