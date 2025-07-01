package ssTable

import (
	"bytes"
	"encoding/binary"
	"errors"
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
