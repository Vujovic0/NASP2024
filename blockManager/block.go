package blockManager

type Block struct {
	filepath  string
	offset    int
	blockType byte
	blockSize int
	data      []byte
}

func InitBlock(filepath string, offset int, blockType byte, blockSize int, data []byte) *Block {
	return &Block{
		filepath:  filepath,
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
	return b.filepath
}

func (b *Block) GetOffset() int {
	return b.offset
}

func (b *Block) GetType() byte {
	return b.blockType
}

func (b *Block) GetSize() int {
	return b.blockSize
}
