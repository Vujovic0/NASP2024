package blockManager

type Block struct {
	filepath  string
	offset    int
	blockType byte
	data      []byte
}

func InitBlock(filepath string, offset int, blockType byte, data []byte) *Block {
	return &Block{
		filepath:  filepath,
		offset:    offset,
		blockType: blockType,
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
