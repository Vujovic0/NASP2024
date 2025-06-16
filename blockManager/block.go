package blockManager

type Block struct {
	filePath  string
	offset    int
	blockType byte
	//Data size, not including padding or header
	dataSize int
	//All data, including header and padding
	data []byte
}

func InitBlock(filepath string, offset int, blockType byte, blockSize int, data []byte) *Block {
	return &Block{
		filePath:  filepath,
		offset:    offset,
		blockType: blockType,
		dataSize:  blockSize,
		data:      data,
	}
}

func (b *Block) GetData() []byte {
	return b.data
}

func (b *Block) GetFilePath() string {
	return b.filePath
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
