package blockManager

import (
	"encoding/binary"
)

// Format: CRC 4B | TYPE 1B | DATASIZE 4B | blockData...
// Type 0 - complete block, type 1 - starting block, type 2 - middle block, type 3 - end block
// Type 2 and 3 don't have entry headers, only block headers
type Block struct {
	filePath  string
	offset    uint64
	blockType byte
	//Data size, not including padding
	dataSize int
	//All data, including header
	data []byte
}

func InitBlock(filepath string, offset uint64, data []byte) *Block {
	dataSize := binary.BigEndian.Uint32(data[5:9])
	return &Block{
		filePath:  filepath,
		offset:    offset,
		blockType: data[4],
		dataSize:  int(dataSize),
		data:      data,
	}
}

func (b *Block) GetData() []byte {
	return b.data
}

func (b *Block) GetFilePath() string {
	return b.filePath
}

func (b *Block) GetOffset() uint64 {
	return b.offset
}

func (b *Block) GetType() byte {
	return b.blockType
}

func (b *Block) GetSize() int {
	return b.dataSize
}
