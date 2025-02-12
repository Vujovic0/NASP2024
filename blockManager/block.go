package blockManager

type Block struct {
	filePath  string
	offset    uint64
	blockType byte
	blockSize int
	data      []byte
}

func InitBlock(filepath string, offset uint64, blockType byte, blockSize int, data []byte) *Block {
	return &Block{
		filePath:  filepath,
		offset:    offset,
		blockType: blockType,
		blockSize: blockSize,
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
	return b.blockSize
}
