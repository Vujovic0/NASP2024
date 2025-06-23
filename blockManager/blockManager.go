package blockManager

import (
	"NASP2024/config"
	"io"
	"os"
)

const (
	BLOCK_HEADER_SIZE = 9
	DATA_HEADER_SIZE  = 29
	HEADER_SIZE       = 38
)

var bc *blockCache[*Block] = InitBlockCache[*Block](50)

var blockSize = config.GlobalBlockSize

// Takes pointer to file and the block that should be written
// First checks if a block with the same filepath and offset is in cache, and adds it if not
// If it is in cache, it shallow copies the new one and assigns it to the old one
// findBlock function moves the block up to be first if it is found
// After adding to cache, it writes to file
func WriteBlock(file *os.File, block *Block) {
	cachedBlock, ok := bc.findBlock(block.GetFilePath(), block.GetOffset())
	if !ok {
		bc.addBlock(block)
	} else {
		*cachedBlock = *block
	}

	file.Seek(int64(block.GetOffset()*uint64(blockSize)), 0)
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
	_, err := file.Read(blockData)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		panic("Error reading file: " + file.Name())
	}
	block = InitBlock(file.Name(), offset, blockData)
	bc.addBlock(block)
	return block
}
