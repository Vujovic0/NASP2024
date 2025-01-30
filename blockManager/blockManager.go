package blockManager

import (
	"encoding/binary"
	"hash/crc32"
	"os"
)

var blockSize = 1024 * 4

// Writes data serialized using big endian. Can work as a generator function when used along keyword "range".
func WriteData(filePath string, data []byte) <-chan *Block {
	ch := make(chan *Block)

	process := func() {
		file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		var dataPointer uint64 = 0
		blockPointer := 5
		blockData := make([]byte, blockSize)
		blockOffset := 0
		var crcValue uint32 = 0
		var blockType byte = 0

		var entryValueSize uint64 = 0
		var entryKeySize uint64 = 0
		var entrySize uint64 = 0
		var entrySizeLeft uint64 = 0
		var block *Block
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
					crcValue = crc32.ChecksumIEEE(append([]byte{blockType}, blockData[4:]...))
					binary.BigEndian.PutUint32(blockData[0:4], crcValue)
					blockData[4] = blockType
					block = InitBlock(filePath, blockOffset, blockType, blockData)
					file.Write(block.GetData())
					ch <- block
					break
				}
				continue
			} else if blockPointer != 5 {
				blockType = 0
				crcValue = crc32.ChecksumIEEE(append([]byte{blockType}, blockData[4:]...))
				binary.BigEndian.PutUint32(blockData[0:4], crcValue)
				blockData[4] = blockType
				block = InitBlock(filePath, blockOffset, blockType, blockData)
			} else {
				if newEntryCheck {
					blockType = 1
				}
				newEntryCheck = false
				if uint64(blockSize-5) >= entrySizeLeft {
					blockType = 3
					crcValue = crc32.ChecksumIEEE(append([]byte{blockType}, data[dataPointer:dataPointer+uint64(entrySizeLeft)]...))
					binary.BigEndian.PutUint32(blockData[0:4], crcValue)
					blockData[4] = blockType
					copy(blockData[5:], data[dataPointer:uint64(dataPointer)+entrySizeLeft])
					block = InitBlock(filePath, blockOffset, blockType, blockData)
					newEntryCheck = true
					dataPointer += entrySizeLeft
				} else {
					blockType = 2
					entrySizeLeft -= uint64(blockSize - 5)
					crcValue = crc32.ChecksumIEEE(append([]byte{blockType}, data[dataPointer:dataPointer+uint64(blockSize-5)]...))
					binary.BigEndian.PutUint32(blockData[0:4], crcValue)
					blockData[4] = blockType
					copy(blockData[5:], data[dataPointer:uint64(dataPointer)+uint64(blockSize-5)])
					dataPointer += uint64(blockSize - 5)
					block = InitBlock(filePath, blockOffset, blockType, blockData)
				}
			}
			blockData = make([]byte, blockSize)
			blockPointer = 5
			blockOffset += 1
			file.Write(block.GetData())
			ch <- block
		}
	}

	go func() {
		process()
		close(ch)
	}()

	return ch
}

func WriteBlock(block *Block) {
	file, err := os.OpenFile(block.GetFilePath(), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	file.Seek(int64(block.GetOffset()*blockSize), 0)
	file.Write(block.GetData())
}

func ReadBlock(filePath string, offset int) *Block {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	file.Seek(int64(offset)*int64(blockSize), 0)
	blockData := make([]byte, blockSize)
	file.Read(blockData)
	block := InitBlock(filePath, offset, blockData[4], blockData)
	return block
}
