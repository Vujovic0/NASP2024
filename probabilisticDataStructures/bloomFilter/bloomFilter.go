package bloomFilter

import (
	"encoding/binary"
	"fmt"
	"os"
)

type BloomFilter struct {
	register     []byte
	registerSize uint
	hash         []HashWithSeed
}

func NewBloomFilter(m uint, hash []HashWithSeed) *BloomFilter {
	register := make([]byte, m)
	return &BloomFilter{hash: hash, register: register, registerSize: m}
}

func MakeBloomFilter(array []string, falsePositive float64) *BloomFilter {
	numberOfElem := len(array)
	m := CalculateM(numberOfElem, falsePositive)
	k := CalculateK(numberOfElem, m)
	hash := CreateHashFunctions(k)
	bf := NewBloomFilter(m, hash)
	for _, data := range array {
		for _, hfn := range bf.hash {
			byteSlice := []byte(data)
			hashValue := hfn.Hash(byteSlice)
			bitIndex := hashValue % uint64(bf.registerSize*8)
			byteIndex := bitIndex / 8
			bitPosition := bitIndex % 8
			bf.register[byteIndex] |= (1 << bitPosition)
		}
	}
	return bf
}

func SearchData(bf *BloomFilter, data string) bool {
	byteSlice := []byte(data)
	for _, hfn := range bf.hash {
		hashValue := hfn.Hash(byteSlice)
		bitIndex := hashValue % uint64(bf.registerSize*8)
		byteIndex := bitIndex / 8
		bitPosition := bitIndex % 8
		if bf.register[byteIndex]&(1<<bitPosition) == 0 {
			return false
		}
	}
	return true
}

func Serialize(bf *BloomFilter, filename string) error {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error while opening file: %v", err)
	}
	defer file.Close()
	// Write the register size
	if err := binary.Write(file, binary.LittleEndian, uint64(bf.registerSize)); err != nil {
		return fmt.Errorf("error while writing register size: %v", err)
	}
	// Write the register data
	if err := binary.Write(file, binary.LittleEndian, bf.register); err != nil {
		return fmt.Errorf("error while writing register: %v", err)
	}
	// Write the number of hash functions
	k := uint64(len(bf.hash))
	if err := binary.Write(file, binary.LittleEndian, k); err != nil {
		return fmt.Errorf("error while writing number of hash functions: %v", err)
	}
	// Serialize each hash function's seed
	for _, hfn := range bf.hash {
		// Write the seed length
		seedLen := uint32(len(hfn.Seed))
		if err := binary.Write(file, binary.LittleEndian, seedLen); err != nil {
			return fmt.Errorf("error while writing hash function seed length: %v", err)
		}
		// Write the seed itself
		if err := binary.Write(file, binary.LittleEndian, hfn.Seed); err != nil {
			return fmt.Errorf("error while writing hash function seed: %v", err)
		}
	}
	return nil
}

// Deserialize reads the BloomFilter from a binary file
func Deserialize(filename string) (*BloomFilter, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()
	bf := &BloomFilter{}
	// Read register size
	if err := binary.Read(file, binary.LittleEndian, &bf.registerSize); err != nil {
		return nil, fmt.Errorf("error while reading register size: %v", err)
	}
	// Read the register data
	bf.register = make([]byte, bf.registerSize)
	if err := binary.Read(file, binary.LittleEndian, bf.register); err != nil {
		return nil, fmt.Errorf("error while reading register: %v", err)
	}
	// Read number of hash functions
	var k uint64
	if err := binary.Read(file, binary.LittleEndian, &k); err != nil {
		return nil, fmt.Errorf("error while reading number of hash functions: %v", err)
	}
	bf.hash = make([]HashWithSeed, k)
	// Read each hash function's seed
	for i := uint64(0); i < k; i++ {
		var seedLen uint32
		// Read the seed length
		if err := binary.Read(file, binary.LittleEndian, &seedLen); err != nil {
			return nil, fmt.Errorf("error while reading hash function seed length: %v", err)
		}
		// Read the seed
		seed := make([]byte, seedLen)
		if err := binary.Read(file, binary.LittleEndian, seed); err != nil {
			return nil, fmt.Errorf("error while reading hash function seed: %v", err)
		}
		// Create HashWithSeed and store it
		bf.hash[i] = HashWithSeed{Seed: seed}
	}
	return bf, nil
}
