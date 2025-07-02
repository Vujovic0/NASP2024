package ssTable

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"os"
	"strconv"
	"strings"
)

// Dictionary Encoder
type Dictionary struct {
	EncodeMap map[string]uint64
	DecodeMap map[uint64]string
	counter   uint64
}

func NewDictionary() *Dictionary {
	return &Dictionary{
		EncodeMap: make(map[string]uint64),
		DecodeMap: make(map[uint64]string),
		counter:   0,
	}
}

func (d *Dictionary) Encode(value string) []byte {
	if idx, ok := d.EncodeMap[value]; ok {
		return binary.AppendUvarint(nil, idx)
	}
	d.EncodeMap[value] = d.counter
	d.DecodeMap[d.counter] = value
	d.counter++
	return binary.AppendUvarint(nil, d.counter-1)
}

func (d *Dictionary) Decode(encoded []byte) (string, error) {
	idx, _ := binary.Uvarint(encoded)
	val, ok := d.DecodeMap[idx]
	if !ok {
		return "", ErrDictionaryDecode
	}
	return val, nil
}

// DecodeBytes - decodes a stream of encoded values using varint format
func (d *Dictionary) DecodeBytes(encoded []byte) ([]string, error) {
	var result []string
	buf := bytes.NewReader(encoded)
	for {
		idx, err := binary.ReadUvarint(buf)
		if err != nil {
			break
		}
		val, ok := d.DecodeMap[idx]
		if !ok {
			return nil, ErrDictionaryDecode
		}
		result = append(result, val)
	}
	return result, nil
}

var ErrDictionaryDecode = errors.New("cannot decode dictionary value")

// Apply dictionary encoding on array of values
func CompressWithDictionary(values []string) ([]byte, *Dictionary) {
	var buf bytes.Buffer
	dict := NewDictionary()
	for _, val := range values {
		enc := dict.Encode(val)
		buf.Write(enc)
	}
	return buf.Bytes(), dict
}

// Varint compression for list of uint64
func CompressWithVarint(nums []uint64) []byte {
	var buf bytes.Buffer
	for _, n := range nums {
		buf.Write(binary.AppendUvarint(nil, n))
	}
	return buf.Bytes()
}

func ConvertDictStringToBinary(m map[string]string) *Dictionary {
	d := NewDictionary()
	for k, v := range m {
		id, _ := strconv.ParseUint(k, 10, 64)
		d.DecodeMap[id] = v
		d.EncodeMap[v] = id
	}
	return d
}

func LoadDictionaryFromFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var dict map[string]string
	decoder := gob.NewDecoder(f)
	err = decoder.Decode(&dict)
	if err != nil {
		return nil, err
	}
	return dict, nil
}

func DecompressValue(data []byte, dict *Dictionary) (string, error) {
	var result strings.Builder
	segments := bytes.Split(data, []byte("|"))
	for _, seg := range segments {
		if len(seg) == 0 {
			continue
		}
		word, err := dict.Decode(seg)
		if err != nil {
			return "", err
		}
		result.WriteString(word)
	}
	return result.String(), nil
}

func DecompressValues(values []string, dict map[uint64]string) []string {
	var decompressed []string
	for _, val := range values {
		var result strings.Builder
		parts := strings.Split(val, "|")
		for _, part := range parts {
			if len(part) == 0 {
				continue
			}
			encoded := []byte(part)
			idx, _ := binary.Uvarint(encoded)
			word := dict[idx]
			result.WriteString(word)
		}
		decompressed = append(decompressed, result.String())
	}
	return decompressed
}

func DecompressSingle(val string, dict map[string]string) string {
	for code, word := range dict {
		val = strings.ReplaceAll(val, code, word)
	}
	return val
}

// SaveDictionaryToFile - helper to persist dictionary to disk
func SaveDictionaryToFile(dict *Dictionary, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	data := make(map[string]string)
	for k, v := range dict.EncodeMap {
		data[string(binary.AppendUvarint(nil, v))] = k
	}

	encoder := gob.NewEncoder(f)
	return encoder.Encode(data)
}

func DecompressWithDictionary(data []byte, dict *Dictionary) ([]string, error) {
	parts := bytes.Split(data, []byte("|"))
	result := make([]string, 0, len(parts)-1)
	for _, p := range parts {
		if len(p) == 0 {
			continue
		}
		val, err := dict.Decode(p)
		if err != nil {
			return nil, err
		}
		result = append(result, val)
	}
	return result, nil
}
