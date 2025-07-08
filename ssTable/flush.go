package ssTable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Vujovic0/NASP2024/config"
)

func Flush(elements []Element, sstFilename string) error {
	var values []string
	for _, el := range elements {
		values = append(values, el.Value)
	}

	compressedData, dict := CompressWithDictionary(values)
	split := bytes.Split(compressedData, []byte("|"))
	if len(split) != len(elements) {
		return errors.New("compression mismatch with element count")
	}
	for i := range elements {
		elements[i].Value = string(split[i])
	}

	var ptrElements []*Element
	for i := range elements {
		ptrElements = append(ptrElements, &elements[i])
	}
	WriteMergedSSTable(ptrElements)

	dictFile := strings.Replace(sstFilename, ".sst", ".dict", 1)
	return SaveDictionaryToFile(dict, dictFile)
}

func WriteMergedSSTable(entries []*Element) {
	var values []string
	for _, e := range entries {
		values = append(values, e.Value)
	}

	compressedData, dict := CompressWithDictionary(values)
	split := bytes.Split(compressedData, []byte("|"))
	for i, e := range entries {
		if i < len(split) {
			e.Value = string(split[i])
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
