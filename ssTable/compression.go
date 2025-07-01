package ssTable

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"os"
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

var ErrDictionaryDecode = errors.New("cannot decode dictionary value")

// Apply dictionary encoding on array of values
func CompressWithDictionary(values []string) ([]byte, *Dictionary) {
	var buf bytes.Buffer
	dict := NewDictionary()
	for _, val := range values {
		enc := dict.Encode(val)
		buf.Write(enc)
		buf.WriteByte('|') // delimiter
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

func DecompressSingle(val string, dict map[string]string) string {
	for code, word := range dict {
		val = strings.ReplaceAll(val, code, word)
	}
	return val
}
