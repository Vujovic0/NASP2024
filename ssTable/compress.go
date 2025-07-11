package ssTable

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"strings"
)

func CompressWithDictionary(values []string) ([]byte, map[string]int) {
	dict := make(map[string]int)
	dictList := make([]string, 0)
	index := 1

	for _, v := range values {
		words := strings.Fields(v)
		for _, word := range words {
			if _, exists := dict[word]; !exists {
				dict[word] = index
				dictList = append(dictList, word)
				index++
			}
		}
	}

	var compressed []string
	for _, v := range values {
		words := strings.Fields(v)
		var encoded []string
		for _, word := range words {
			encoded = append(encoded, fmt.Sprintf("%d", dict[word]))
		}
		compressed = append(compressed, strings.Join(encoded, ","))
	}

	return []byte(strings.Join(compressed, "|")), dict
}

func DecompressValue(value []byte, dict map[int]string) string {
	parts := strings.Split(string(value), ",")
	var decoded []string
	for _, p := range parts {
		if p == "" {
			continue
		}
		var idx int
		fmt.Sscanf(p, "%d", &idx)
		decoded = append(decoded, dict[idx])
	}
	return strings.Join(decoded, " ")
}

func SaveDictionaryToFile(dict map[string]int, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	enc := gob.NewEncoder(file)
	return enc.Encode(dict)
}

func LoadDictionaryFromFile(path string) (map[int]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	raw := make(map[string]int)
	dec := gob.NewDecoder(file)
	err = dec.Decode(&raw)
	if err != nil {
		return nil, err
	}

	reverse := make(map[int]string)
	for word, idx := range raw {
		reverse[idx] = word
	}

	return reverse, nil
}

func SerializeEntryWithCompression(key string, value []byte, tombstone bool, timestamp uint64, dict *Dictionary, enableCompression bool) []byte {
	buf := make([]byte, 0, 64)
	tmp := make([]byte, 10)

	// CRC (dummy 4 bytes)
	buf = append(buf, 0, 0, 0, 0)

	// Timestamp (varint)
	n := binary.PutUvarint(tmp, timestamp)
	buf = append(buf, tmp[:n]...)

	// Tombstone (1 byte)
	if tombstone {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}

	// Key serialization
	keyID := dict.GetOrAdd(key)
	n = binary.PutUvarint(tmp, uint64(keyID))
	buf = append(buf, tmp[:n]...)

	// Value (compressed)
	if !tombstone {
		n = binary.PutUvarint(tmp, uint64(len(value)))
		buf = append(buf, tmp[:n]...)
		buf = append(buf, value...)
	}

	return buf
}
