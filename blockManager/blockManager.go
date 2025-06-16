package blockManager

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
)

const (
	BLOCK_HEADER_SIZE = 9
	DATA_HEADER_SIZE  = 37
	HEADER_SIZE       = 46
)

var bc *blockCache[*Block] = InitBlockCache[*Block](50)

var blockSize = config.GlobalBlockSize

// Takes pointer to file and the block that should be written
// First checks if a block with the same filepath and offset is in cache, and adds it if not
// If it is in cache, it shallow copies the new one and assigns it to the old one
// After adding to cache, it writes to file
func WriteBlock(file *os.File, block *Block) {
	cachedBlock, ok := bc.findBlock(block.GetFilePath(), block.GetOffset())
	if !ok {
		bc.addBlock(block)
	} else {
		*cachedBlock = *block
	}

	file.Seek(int64(block.GetOffset()*blockSize), 0)
	file.Write(block.GetData())
}

// Takes pointer to file and offset of block that it should read. Returns pointer to read block
// First checks if the block is in cache, if yes it doesn't read from disk but just returns from cache
// If block is not in cache it reads data and constructs the block object
func ReadBlock(file *os.File, offset uint64) *Block {

	block, ok := bc.findBlock(file.Name(), offset)
	if ok {
		return block
	}

	file.Seek(int64(offset)*int64(blockSize), 0)
	blockData := make([]byte, blockSize)
	file.Read(blockData)
	dataSize := binary.BigEndian.Uint32(blockData[5:9])
	block = InitBlock(file.Name(), offset, blockData[4], int(dataSize), blockData)
	return block
}

// Returns a list of keys inside a block or multiple blocks that are written on disk
// !!!Doesn't work if blocks aren't written on disk!!!
func GetKeys(block *Block) ([]string, error) {
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

		file, err := os.OpenFile(block.GetFilePath(), os.O_RDONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("Failed to open file: " + block.GetFilePath())
		}

		for keySize > 0 {
			keyBytes = append(keyBytes, block.GetData()[blockPointer:min(keySize, uint64(blockSize))]...)
			keySize -= uint64(blockSize)
			blockPointer = 9
			block = ReadBlock(file, block.GetOffset()+1)
		}
		keys = append(keys, string(keyBytes))
	} else {
		panic("This isn't the first block of data!")
	}

	return keys, nil
}
