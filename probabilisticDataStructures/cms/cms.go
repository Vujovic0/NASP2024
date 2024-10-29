package cms

type CountMinSketch struct {
	register [][]uint32
	hash     []HashWithSeed
}

func NewCountMinSketch(m uint, k uint, hash []HashWithSeed) *CountMinSketch {
	registerTable := make([][]uint32, k)
	for i := range registerTable {
		registerTable[i] = make([]uint32, m)
	}
	return &CountMinSketch{hash: hash, register: registerTable}
}

func GetRegister(bf *CountMinSketch) [][]uint32 {
	return bf.register
}

func SetRegister(bf *CountMinSketch, array [][]uint32) {
	bf.register = array
}

func MakeCountMinSketch(array []string, epsilon float64, delta float64) *CountMinSketch {
	m := CalculateM(epsilon)
	k := CalculateK(delta)
	hash := CreateHashFunctions(k)
	bf := NewCountMinSketch(m, k, hash)
	for _, data := range array {
		for index, hfn := range bf.hash {
			byteSlice := []byte(data)
			hashValue := hfn.Hash(byteSlice) % uint64(m)
			bf.register[index][hashValue]++
		}
	}
	return bf
}

func SearchData(bf *CountMinSketch, data string) int {
	byteSlice := []byte(data)
	counts := make([]int, len(bf.hash))
	for index, hfn := range bf.hash {
		hashValue := hfn.Hash(byteSlice)
		counts[index] = int(bf.register[index][hashValue%uint64(len(bf.register[0]))])
	}
	count := int(^uint(0) >> 1)
	for _, el := range counts {
		if count > el {
			count = el
		}
	}
	return count
}
