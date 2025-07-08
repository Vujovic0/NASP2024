package ssTable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/Vujovic0/NASP2024/blockManager"
	"github.com/Vujovic0/NASP2024/config"
)

func WriteFooter(file *os.File, indexOffset, summaryOffset, boundOffset uint64) error {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, indexOffset)
	if err != nil {
		return err
	}
	err = binary.Write(&buf, binary.LittleEndian, summaryOffset)
	if err != nil {
		return err
	}
	err = binary.Write(&buf, binary.LittleEndian, boundOffset)
	if err != nil {
		return err
	}

	_, err = file.Write(buf.Bytes())
	return err
}

// WriteIndexBlock writes the index block mapping keys to data block offsets
func WriteIndexBlock(file *os.File, elements []Element, currentOffset uint64) (uint64, error) {
	offset := currentOffset

	var buf bytes.Buffer
	for _, el := range elements {
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(el.Key))); err != nil {
			return 0, err
		}
		if _, err := buf.Write([]byte(el.Key)); err != nil {
			return 0, err
		}
		// dummy offset, možeš kasnije proširiti
		if err := binary.Write(&buf, binary.LittleEndian, uint64(0)); err != nil {
			return 0, err
		}
	}

	block := blockManager.NewBlock(0)
	block.Add(buf.Bytes())
	block.Offset = offset

	err := blockManager.WriteBlock(file, block)
	if err != nil {
		return 0, err
	}

	return offset + uint64(len(block.GetData())), nil
}

// WriteSummaryBlock writes a summary block (e.g. sampled keys for faster lookup)
func WriteSummaryBlock(file *os.File, elements []Element, currentOffset uint64) (uint64, error) {
	offset := currentOffset

	var buf bytes.Buffer
	step := 10
	for i := 0; i < len(elements); i += step {
		el := elements[i]
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(el.Key))); err != nil {
			return 0, err
		}
		if _, err := buf.Write([]byte(el.Key)); err != nil {
			return 0, err
		}
	}

	block := blockManager.NewBlock(0)
	block.Add(buf.Bytes())
	block.Offset = offset

	err := blockManager.WriteBlock(file, block)
	if err != nil {
		return 0, err
	}

	return offset + uint64(len(block.GetData())), nil
}

// WriteBoundBlock writes the maximum key in the SSTable (bound block)
func WriteBoundBlock(file *os.File, elements []Element, currentOffset uint64) (uint64, error) {
	offset := currentOffset

	maxKey := elements[len(elements)-1].Key

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(maxKey))); err != nil {
		return 0, err
	}
	if _, err := buf.Write([]byte(maxKey)); err != nil {
		return 0, err
	}

	block := blockManager.NewBlock(0)
	block.Add(buf.Bytes())
	block.Offset = offset

	err := blockManager.WriteBlock(file, block)
	if err != nil {
		return 0, err
	}

	return offset + uint64(len(block.GetData())), nil
}

func Flush(elements []Element, sstFilename string) error {
	// Sortiraj elemente po ključu ([]byte, ne string)
	sort.Slice(elements, func(i, j int) bool {
		return bytes.Compare([]byte(elements[i].Key), []byte(elements[j].Key)) < 0
	})

	file, err := os.Create(sstFilename)
	if err != nil {
		return err
	}
	defer file.Close()

	var blocks []*blockManager.Block
	currentOffset := uint64(0)

	// Piši svaki element kao poseban blok i postavi offset
	for _, el := range elements {
		var buf bytes.Buffer

		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(el.Key))); err != nil {
			return err
		}
		if _, err := buf.Write([]byte(el.Key)); err != nil {
			return err
		}

		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(el.Value))); err != nil {
			return err
		}
		if _, err := buf.Write(el.Value); err != nil {
			return err
		}

		block := blockManager.NewBlock(0)
		block.Add(buf.Bytes())
		block.Offset = currentOffset

		err := blockManager.WriteBlock(file, block)
		if err != nil {
			return err
		}

		blocks = append(blocks, block)
		currentOffset += uint64(len(block.GetData()))
	}

	// Piši index block
	indexOffset, err := WriteIndexBlock(file, elements, currentOffset)
	if err != nil {
		return err
	}
	currentOffset = indexOffset

	// Piši summary block
	summaryOffset, err := WriteSummaryBlock(file, elements, currentOffset)
	if err != nil {
		return err
	}
	currentOffset = summaryOffset

	// Piši bound block
	boundOffset, err := WriteBoundBlock(file, elements, currentOffset)
	if err != nil {
		return err
	}
	currentOffset = boundOffset

	// Piši footer
	err = WriteFooter(file, indexOffset, summaryOffset, boundOffset)
	if err != nil {
		return err
	}

	// Opcionalno: kompresuj i sačuvaj dictionary
	var values []string
	for _, el := range elements {
		values = append(values, string(el.Value))
	}
	_, dict := CompressWithDictionary(values)
	dictFile := strings.Replace(sstFilename, ".sst", ".dict", 1)
	return SaveDictionaryToFile(dict, dictFile)
}

func WriteMergedSSTable(entries []*Element) {
	var values []string
	for _, e := range entries {
		values = append(values, string(e.Value))
	}

	compressedData, dict := CompressWithDictionary(values)
	split := bytes.Split(compressedData, []byte("|"))
	for i, e := range entries {
		if i < len(split) {
			e.Value = split[i]
		}
	}

	var data []byte
	for _, e := range entries {
		entry := initEntry(0, e.Tombstone, uint64(e.Timestamp), []byte(e.Key), []byte(e.Value))
		data = append(data, SerializeEntry(entry, e.Tombstone)...)
	}

	last := entries[len(entries)-1]
	lastKeyBytes := []byte(last.Key)
	lastKeySize := uint64(len(lastKeyBytes))
	var lastBuf []byte
	if config.VariableEncoding {
		lastBuf = binary.AppendUvarint(lastBuf, lastKeySize)
	} else {
		lastBuf = binary.LittleEndian.AppendUint64(lastBuf, lastKeySize)
	}
	lastBuf = append(lastBuf, lastKeyBytes...)

	CreateCompactSSTable(data, lastBuf, 2, 4)

	err := SaveDictionaryToFile(dict, "data/dictionary.dict")
	if err != nil {
		panic("Failed to save dict: " + err.Error())
	}
}

func EncodeLastEntry(elem Element) []byte {
	buf := make([]byte, 0)
	buf = binary.AppendUvarint(buf, uint64(len(elem.Key)))
	buf = append(buf, []byte(elem.Key)...)
	return buf
}

func EncodeData(elems []Element) []byte {
	var buf bytes.Buffer
	for _, elem := range elems {
		crc := make([]byte, 4)
		binary.Write(&buf, binary.LittleEndian, crc)
		binary.Write(&buf, binary.BigEndian, elem.Timestamp)
		tomb := uint64(0)
		if elem.Tombstone {
			tomb = 1
		}
		binary.Write(&buf, binary.BigEndian, tomb)
		binary.Write(&buf, binary.BigEndian, uint64(len(elem.Key)))
		binary.Write(&buf, binary.BigEndian, uint64(len(elem.Value)))
		buf.Write([]byte(elem.Key))
		buf.Write([]byte(elem.Value))
	}
	return buf.Bytes()
}

func PromoteLevel(currentLevel int) string {
	nextLevel := currentLevel + 1
	os.MkdirAll(fmt.Sprintf("data/L%d", nextLevel), 0755)
	return fmt.Sprintf("data/L%d", nextLevel)
}
