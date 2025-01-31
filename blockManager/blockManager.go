package blockManager

import (
	"encoding/binary"
	"hash/crc32"
	"os"
)

const (
	BLOCK_HEADER_SIZE = 9
	DATA_HEADER_SIZE  = 37
	HEADER_SIZE       = 46
)

var blockSize = 1024 * 4

// Writes data serialized using big endian. Can work as a generator function when used along keyword "range".
func WriteData(filePath string, data []byte, blockCache *blockCache) <-chan *Block {
	ch := make(chan *Block)

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
		var block *Block
		var newEntryCheck bool = true
		var keyCrc uint32

		for dataPointer < uint64(len(data)) {

			if newEntryCheck {
				entryKeySize = binary.BigEndian.Uint64(data[dataPointer+21 : dataPointer+29])
				entryValueSize = binary.BigEndian.Uint64(data[dataPointer+29 : dataPointer+37])
				keyCrc = crc32.ChecksumIEEE(data[dataPointer : dataPointer+entryKeySize])
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

					block = InitBlock(filePath, blockOffset, blockType, int(blockPointer-9), blockData)
					blockCache.addBlock(int(keyCrc), block)
					file.Write(block.GetData())
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

				block = InitBlock(filePath, blockOffset, blockType, blockPointer-9, blockData)
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

					block = InitBlock(filePath, blockOffset, blockType, int(entrySizeLeft), blockData)
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
					block = InitBlock(filePath, blockOffset, blockType, blockSize-9, blockData)
				}
			}
			blockData = make([]byte, blockSize)
			blockPointer = 9
			blockOffset += 1
			blockCache.addBlock(int(keyCrc), block)
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
	dataSize := binary.BigEndian.Uint32(blockData[5:9])
	block := InitBlock(filePath, offset, blockData[4], int(dataSize), blockData)
	return block
}

// Returns a list of keys inside a block or multiple blocks that are written on disk
func GetKeys(block *Block) []string {
	keys := make([]string, 0)
	blockPointer := 9 + 21
	var keySize uint64 = 0
	var valueSize uint64 = 0
	if block.GetType() == 0 {
		for blockPointer < block.GetSize()+9 {
			keySize = uint64(binary.BigEndian.Uint64(block.GetData()[blockPointer : blockPointer+8]))
			valueSize = uint64(binary.BigEndian.Uint64(block.GetData()[blockPointer : blockPointer+16]))
			blockPointer += 16
			keys = append(keys, string(block.GetData()[blockPointer:blockPointer+int(keySize)]))
			blockPointer += int(keySize) + int(valueSize)
		}
	} else if block.GetType() == 1 {
		keyBytes := make([]byte, 0)
		keySize = uint64(binary.BigEndian.Uint64(block.GetData()[blockPointer : blockPointer+8]))
		blockPointer += 16
		for keySize > 0 {
			keyBytes = append(keyBytes, block.GetData()[blockPointer:min(keySize, uint64(blockSize))]...)
			keySize -= uint64(blockSize)
			blockPointer = 9
			block = ReadBlock(block.GetFilePath(), block.GetOffset()+1)
		}
		keys = append(keys, string(keyBytes))
	} else {
		panic("This isn't the first block of data!")
	}

	return keys
}
