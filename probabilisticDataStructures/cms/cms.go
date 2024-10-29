package cms

import (
	"encoding/binary"
	"fmt"
	"os"
)

type CountMinSketch struct {
	register     [][]uint32
	registerSize uint
	hash         []HashWithSeed
}

func NewCountMinSketch(m uint, k uint, hash []HashWithSeed) *CountMinSketch {
	registerTable := make([][]uint32, k)
	for i := range registerTable {
		registerTable[i] = make([]uint32, m)
	}
	return &CountMinSketch{hash: hash, register: registerTable, registerSize: m}
}

func MakeCountMinSketch(array []string, epsilon float64, delta float64) *CountMinSketch {
	m := CalculateM(epsilon)
	k := CalculateK(delta)
	hash := CreateHashFunctions(k)
	cms := NewCountMinSketch(m, k, hash)
	cms = UpdateCountMinSketch(cms, array)
	return cms
}

func SearchData(cms *CountMinSketch, data string) int {
	byteSlice := []byte(data)
	counts := make([]int, len(cms.hash))
	for index, hfn := range cms.hash {
		hashValue := hfn.Hash(byteSlice)
		counts[index] = int(cms.register[index][hashValue%uint64(cms.registerSize)])
	}
	count := int(^uint(0) >> 1)
	for _, el := range counts {
		if count > el {
			count = el
		}
	}
	return count
}

func UpdateCountMinSketch(cms *CountMinSketch, array []string) *CountMinSketch {
	for _, data := range array {
		for index, hfn := range cms.hash {
			byteSlice := []byte(data)
			hashValue := hfn.Hash(byteSlice) % uint64(cms.registerSize)
			cms.register[index][hashValue]++
		}
	}
	return cms
}

func Serialize(cms *CountMinSketch, filename string) error {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error while opening file: %v", err)
	}
	defer file.Close()
	// Write the register size
	if err := binary.Write(file, binary.LittleEndian, uint64(cms.registerSize)); err != nil {
		return fmt.Errorf("error while writing register size: %v", err)
	}
	// Write the number of hash functions
	k := uint(len(cms.hash))
	if err := binary.Write(file, binary.LittleEndian, uint64(k)); err != nil {
		return fmt.Errorf("error while writing number of hash functions: %v", err)
	}
	// Serialize each hash function and its seed
	for _, hfn := range cms.hash {
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
	// Write the registers
	for _, reg := range cms.register {
		if err := binary.Write(file, binary.LittleEndian, reg); err != nil {
			return fmt.Errorf("error while writing register: %v", err)
		}
	}
	return nil
}

func Deserialize(file *os.File) (*CountMinSketch, error) {
	cms := &CountMinSketch{}
	// Read register size
	if err := binary.Read(file, binary.LittleEndian, &cms.registerSize); err != nil {
		return nil, fmt.Errorf("error while reading register size: %v", err)
	}
	// Read number of hash functions
	var k uint64
	if err := binary.Read(file, binary.LittleEndian, &k); err != nil {
		return nil, fmt.Errorf("error while reading number of hash functions: %v", err)
	}
	cms.hash = make([]HashWithSeed, k)
	cms.register = make([][]uint32, k)
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
		cms.hash[i] = HashWithSeed{Seed: seed}
	}
	// Read the registers
	for i := uint64(0); i < k; i++ {
		cms.register[i] = make([]uint32, cms.registerSize)
		if err := binary.Read(file, binary.LittleEndian, cms.register[i]); err != nil {
			return nil, fmt.Errorf("error while reading register: %v", err)
		}
	}
	return cms, nil
}
