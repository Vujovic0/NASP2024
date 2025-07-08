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
				Value:     []byte(decoded),
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
			Value:     val,
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
		parts := strings.Split(string(entries[i].Value), "|")
		var val strings.Builder
		for _, p := range parts {
			if len(p) == 0 {
				continue
			}
			idx, _ := binary.Uvarint([]byte(p))
			word := decoded[idx]
			val.WriteString(word)
		}
		entries[i].Value = []byte(val.String())
	}

	return entries, nil
}

func findCompact(path string, key []byte) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	blockOffset := uint64(0)
	var (
		dataBlockCheck bool = true
		tombstone      bool
		keyBytes       []byte
		valueBytes     []byte
		keySizeLeft    uint64
		valueSizeLeft  uint64
		lastValue      []byte
	)

	for {
		block := blockManager.ReadBlock(file, blockOffset)
		if block == nil {
			break
		}

		switch block.GetType() {
		case 0:
			keys, values, err := GetKeysType0(block, dataBlockCheck, 0)
			if err != nil {
				return nil, err
			}
			index := FindLastSmallerKey(key, keys, dataBlockCheck, false)
			if index == -2 {
				if len(lastValue) == 0 {
					return nil, nil
				}
				return lastValue, nil
			}
			if index != -1 {
				return values[index], nil
			}
			lastValue = values[len(values)-1]
		case 1:
			keyBytes, valueBytes, keySizeLeft, valueSizeLeft, err = GetKeysType1(block, dataBlockCheck, 0)
			if err != nil {
				return nil, err
			}
			tombstone = dataBlockCheck && len(valueBytes) == 0 && valueSizeLeft == 0
		case 2:
			var k, v []byte
			k, v, keySizeLeft, valueSizeLeft, err = GetKeysType2(block, keySizeLeft, valueSizeLeft, tombstone, dataBlockCheck)
			if err != nil {
				return nil, err
			}
			keyBytes = append(keyBytes, k...)
			valueBytes = append(valueBytes, v...)
		case 3:
			var k, v []byte
			k, v, err = GetKeysType3(block, keySizeLeft, valueSizeLeft, tombstone, dataBlockCheck)
			if err != nil {
				return nil, err
			}
			keyBytes = append(keyBytes, k...)
			valueBytes = append(valueBytes, v...)
			cmp := bytes.Compare(keyBytes, key)
			if cmp == 0 {
				return valueBytes, nil
			} else if cmp > 0 {
				if len(lastValue) == 0 {
					return nil, nil
				}
				return lastValue, nil
			}
			lastValue = valueBytes
			keyBytes = keyBytes[:0]
			valueBytes = valueBytes[:0]
			keySizeLeft, valueSizeLeft = 0, 0
			tombstone = false
		}
		blockOffset++
	}
	return lastValue, nil
}
