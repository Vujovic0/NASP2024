package bloomFilter

type BloomFilter struct {
	register []byte
	hash     []HashWithSeed
}

func NewBloomFilter(m uint, hash []HashWithSeed) *BloomFilter {
	register := make([]byte, m)
	return &BloomFilter{hash: hash, register: register}
}

func GetRegister(bf *BloomFilter) []byte {
	return bf.register
}

func SetRegister(bf *BloomFilter, array []byte) {
	bf.register = array
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
			bitIndex := hashValue % uint64(len(bf.register)*8)
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
		bitIndex := hashValue % uint64(len(bf.register)*8)
		byteIndex := bitIndex / 8
		bitPosition := bitIndex % 8
		if bf.register[byteIndex]&(1<<bitPosition) == 0 {
			return false
		}
	}
	return true
}
