package ssTable

import (
	"bytes"
	"encoding/binary"
	"os"
	"strconv"
	"strings"

	"github.com/Vujovic0/NASP2024/blockManager"
	"github.com/Vujovic0/NASP2024/config"
)

func LoadAllElements(path string) []Element {
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	dictPath := strings.Replace(path, ".bin", ".dict", 1)
	dict, _ := LoadDictionaryFromFile(dictPath)

	var elements []Element
	blockOffset := uint64(0)

	for {
		block := blockManager.ReadBlock(file, blockOffset)
		if block == nil {
			break
		}

		blockOffset++
		if block.GetType() != 0 {
			continue
		}

		data := block.GetData()[9 : 9+block.GetSize()]
		buf := bytes.NewBuffer(data)
		for buf.Len() > 0 {
			crc := make([]byte, 4)
			buf.Read(crc)

			timestamp, _ := binary.ReadVarint(buf)
			tombstone, _ := binary.ReadUvarint(buf)
			keyLen, _ := binary.ReadUvarint(buf)
			valLen, _ := binary.ReadUvarint(buf)

			key := make([]byte, keyLen)
			buf.Read(key)
			val := make([]byte, valLen)
			buf.Read(val)

			decoded := DecompressSingle(string(val), dict)

			elements = append(elements, Element{
				Key:       string(key),
				Value:     decoded,
				Timestamp: timestamp,
				Tombstone: tombstone == 1,
			})
		}
	}

	return elements
}

func ReadSSTable(path string) ([]Element, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var elements []Element
	reader := bytes.NewReader(data)

	for reader.Len() > 0 {
		// Read size of key
		var keySize uint64
		if config.VariableEncoding {
			keySize, err = binary.ReadUvarint(reader)
		} else {
			var fixedSize uint64
			err = binary.Read(reader, binary.LittleEndian, &fixedSize)
			keySize = fixedSize
		}
		if err != nil {
			break
		}

		key := make([]byte, keySize)
		_, err = reader.Read(key)
		if err != nil {
			break
		}

		// Read size of value
		var valSize uint64
		if config.VariableEncoding {
			valSize, err = binary.ReadUvarint(reader)
		} else {
			var fixedValSize uint64
			err = binary.Read(reader, binary.LittleEndian, &fixedValSize)
			valSize = fixedValSize
		}
		if err != nil {
			break
		}

		val := make([]byte, valSize)
		_, err = reader.Read(val)
		if err != nil {
			break
		}

		// Read timestamp and tombstone
		var timestamp uint64
		err = binary.Read(reader, binary.LittleEndian, &timestamp)
		if err != nil {
			break
		}

		tomb := make([]byte, 1)
		_, err = reader.Read(tomb)
		if err != nil {
			break
		}

		elements = append(elements, Element{
			Key:       string(key),
			Value:     string(val),
			Timestamp: int64(timestamp),
			Tombstone: tomb[0] == 1,
		})
	}

	return elements, nil
}

func ReadSSTableWithDecompression(path string, dictPath string) ([]Element, error) {
	entries, err := ReadSSTable(path)
	if err != nil {
		return nil, err
	}

	dictMap, err := LoadDictionaryFromFile(dictPath)
	if err != nil {
		return nil, err
	}

	decoded := make(map[uint64]string)
	for k, v := range dictMap {
		code, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			continue
		}
		decoded[code] = v
	}

	for i := range entries {
		parts := strings.Split(entries[i].Value, "|")
		var val strings.Builder
		for _, p := range parts {
			if len(p) == 0 {
				continue
			}
			idx, _ := binary.Uvarint([]byte(p))
			word := decoded[idx]
			val.WriteString(word)
		}
		entries[i].Value = val.String()
	}

	return entries, nil
}
