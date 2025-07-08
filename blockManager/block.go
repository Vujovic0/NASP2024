package blockManager

import (
	"bytes"
	"encoding/binary"
)

// Format: CRC 4B | TYPE 1B | DATASIZE 4B | blockData...
// Type 0 - complete block, type 1 - starting block, type 2 - middle block, type 3 - end block
// Type 2 and 3 don't have entry headers, only block headers
type Block struct {
	filePath  string
	Offset    uint64
	blockType byte
	//Data size, not including padding or block header
	dataSize int
	//All data, including header and padding
	data []byte
}

func NewBlock(blockType byte) *Block {
	buf := new(bytes.Buffer)

	// dummy CRC (4B)
	binary.Write(buf, binary.LittleEndian, uint32(0))

	// block type (1B)
	buf.WriteByte(blockType)

	// data size placeholder (4B)
	binary.Write(buf, binary.LittleEndian, uint32(0)) // upisuje se kasnije

	return &Block{
		blockType: blockType,
		dataSize:  0,
		data:      buf.Bytes(),
	}
}

// Dodaj podatke u blok
func (b *Block) Add(data []byte) {
	b.data = append(b.data, data...)
	b.dataSize += len(data)

	// Ažuriraj dataSize u headeru
	binary.LittleEndian.PutUint32(b.data[5:], uint32(b.dataSize))
}

func InitBlock(filepath string, offset uint64, data []byte) *Block {
	dataSize := binary.LittleEndian.Uint32(data[5:9])
	return &Block{
		filePath:  filepath,
		Offset:    offset,
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
	return b.Offset
}

func (b *Block) GetType() byte {
	return b.blockType
}

func (b *Block) GetSize() int {
	return b.dataSize
}
