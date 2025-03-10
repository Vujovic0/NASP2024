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

func WriteBlock(file *os.File, block *Block) {
	_, ok := bc.findBlock(block.GetFilePath(), block.GetOffset())
	if !ok {
		bc.addBlock(block)
	}

	file.Seek(int64(block.GetOffset()*uint64(blockSize)), 0)
	file.Write(block.GetData())
}

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
