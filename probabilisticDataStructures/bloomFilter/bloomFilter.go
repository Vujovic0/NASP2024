package bloomFilter

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
)

type BloomFilter struct {
	register     []byte
	registerSize uint
	hash         []HashWithSeed
	numHashes    int
	stateCheck   int
	numOfElem    int
}

func NewBloomFilter(mBits uint, hash []HashWithSeed) *BloomFilter {
	mBytes := (mBits + 7) / 8 // rounding to the next int
	register := make([]byte, mBytes)
	return &BloomFilter{
		hash:         hash,
		register:     register,
		registerSize: mBits,
		stateCheck:   int(float64(mBits) / math.Log(float64(len(hash))) * 0.6),
		numHashes:    len(hash),
		numOfElem:    0,
	}
}

func MakeBloomFilter(numberOfElem int, falsePositive float64) *BloomFilter {
	m := CalculateM(numberOfElem, falsePositive)
	k := CalculateK(numberOfElem, m)
	hash := CreateHashFunctions(k)
	bf := NewBloomFilter(m, hash)
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

// CHECKING HOW MUCH BITS ARE 1'S IN REGISTER TO SEE THE STATE OF BLOOM FILTER (ASSUMING IN REALITY- for better effectivness)
// <50% --> GOOD
// 50%-70& --> ACCEPTABLE
// >70% --> BAD (high risk of colisions)
func AddData(bf *BloomFilter, array []string) *BloomFilter {
	startState := bf.numOfElem
	confirm := true
	for _, data := range array {
		if bf.numOfElem > bf.stateCheck && confirm {
			fmt.Printf("--WARNING--\nBloom filter is probably at great risk of collisions!\nOut of %d elements you already added %d. Do you want to continue? [y/n]\nYour answer: ", len(array), bf.numOfElem-startState)
		inputLoop:
			for {
				var inputKey string
				fmt.Scan(&inputKey)
				inputKey = strings.TrimSpace(inputKey)
				switch strings.ToLower(inputKey) {
				case "y":
					confirm = false
					break inputLoop
				case "n":
					return bf // Return early if the user decides not to proceed
				default:
					fmt.Println("Your input is invalid! Please enter 'y' or 'n'...")
				}
			}
		}
		byteSlice := []byte(data)
		for _, hfn := range bf.hash {
			hashValue := hfn.Hash(byteSlice)
			bitIndex := hashValue % uint64(bf.registerSize*8)
			byteIndex := bitIndex / 8
			bitPosition := bitIndex % 8
			bf.register[byteIndex] |= (1 << bitPosition)
		}
		bf.numOfElem++
	}
	return bf
}

func SerializeToBytes(bf *BloomFilter) ([]byte, error) {
	buf := make([]byte, 0)

	// Add size of a register (in bits)
	tmp := make([]byte, 8)
	binary.LittleEndian.PutUint64(tmp, uint64(bf.registerSize))
	buf = append(buf, tmp...)

	// Add register (bits like byte set)
	buf = append(buf, bf.register...)

	// Add number of hash functions
	binary.LittleEndian.PutUint64(tmp, uint64(len(bf.hash)))
	buf = append(buf, tmp...)

	// Add every seed value
	for _, hfn := range bf.hash {
		// length
		lenSeed := uint32(len(hfn.Seed))
		tmp4 := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp4, lenSeed)
		buf = append(buf, tmp4...)
		// seed value
		buf = append(buf, hfn.Seed...)
	}
	return buf, nil
}

func DeserializeFromBytes(data []byte) (*BloomFilter, error) {
	bf := &BloomFilter{}
	offset := 0

	// Read registerSize (8 bytes)
	if offset+8 > len(data) {
		return nil, fmt.Errorf("not enough data for register size")
	}
	bf.registerSize = uint(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8

	// Calculate number of bytes in register
	mBytes := (bf.registerSize + 7) / 8
	if offset+int(mBytes) > len(data) {
		return nil, fmt.Errorf("not enough data for register")
	}
	bf.register = make([]byte, mBytes)
	copy(bf.register, data[offset:offset+int(mBytes)])
	offset += int(mBytes)

	// Number of hash functions (8 bytes)
	if offset+8 > len(data) {
		return nil, fmt.Errorf("not enough data for hash function count")
	}
	k := binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	// Raed all hash functions
	bf.hash = make([]HashWithSeed, k)
	for i := uint64(0); i < k; i++ {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("not enough data for seed length")
		}
		seedLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		if offset+int(seedLen) > len(data) {
			return nil, fmt.Errorf("not enough data for seed content")
		}
		seed := make([]byte, seedLen)
		copy(seed, data[offset:offset+int(seedLen)])
		offset += int(seedLen)
		bf.hash[i] = HashWithSeed{Seed: seed}
	}

	bf.numHashes = len(bf.hash)
	bf.stateCheck = int(float64(bf.registerSize) / math.Log(float64(bf.numHashes)) * 0.6)
	bf.numOfElem = 0 // reset number of added elements, we don't know exact state

	return bf, nil
}
