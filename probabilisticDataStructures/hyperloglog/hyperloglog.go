package hyperloglog

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
	"os"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

func firstKbits(value, k uint64) uint64 {
	return value >> (64 - k)
}

func trailingZeroBits(value uint64) int {
	return bits.TrailingZeros64(value)
}

type HyperLogLog struct {
	m        uint64
	p        uint8
	register []uint8
}

func (hll *HyperLogLog) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.register {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HyperLogLog) emptyCount() int {
	sum := 0
	for _, val := range hll.register {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func NewHyperLogLog(p uint8) *HyperLogLog {
	var m uint64 = 1 << p
	register := make([]uint8, m)
	return &HyperLogLog{p: p, m: m, register: register}
}

func MakeHyperLogLog(p uint8) *HyperLogLog {
	hll := NewHyperLogLog(p)
	return hll
}

func UpdateHyperLogLog(hll *HyperLogLog, words []string) *HyperLogLog {
	h := fnv.New64a()
	for i := 0; i < len(words); i++ {
		h.Write([]byte(words[i]))
	}
	hashValue := h.Sum64()
	bucket := firstKbits(hashValue, uint64(hll.p)) % hll.m
	value := trailingZeroBits(hashValue)
	valueUint8 := uint8(value)
	if hll.register[bucket] < valueUint8 {
		hll.register[bucket] = valueUint8
	}
	return hll
}

func GetNumberOfDifferentValues(hll *HyperLogLog) int {
	return int(hll.Estimate())
}

func Serialize(hll *HyperLogLog, filename string) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Println("Error while opening file: ", err)
		return
	}
	defer file.Close()
	// Write the precision
	if err := binary.Write(file, binary.LittleEndian, hll.p); err != nil {
		fmt.Println("Error while writing precision: ", err)
		return
	}
	// Write the register
	if err := binary.Write(file, binary.LittleEndian, hll.register); err != nil {
		fmt.Println("Error while writing register: ", err)
		return
	}
}

func Deserialize(file *os.File) (*HyperLogLog, error) {
	hll := &HyperLogLog{}
	// Read the precision p (1 byte)
	if err := binary.Read(file, binary.LittleEndian, &hll.p); err != nil {
		return nil, fmt.Errorf("error while reading precision: %v", err)
	}
	hll.m = 1 << hll.p // SAME AS WRITING 2^p
	hll.register = make([]uint8, hll.m)
	// Read the register values (m bytes)
	if err := binary.Read(file, binary.LittleEndian, hll.register); err != nil {
		return nil, fmt.Errorf("error while reading register: %v", err)
	}
	return hll, nil
}

func SerializeToBytes(hll *HyperLogLog) ([]byte, error) {
	if hll == nil {
		return nil, fmt.Errorf("HyperLogLog is nil")
	}

	m := uint64(1) << hll.p
	buf := make([]byte, 0, 1+int(m)) // Precision p + m bytes for register

	// Add precision
	buf = append(buf, byte(hll.p))
	// Add register
	buf = append(buf, hll.register...)

	return buf, nil
}

func DeserializeFromBytes(data []byte) (*HyperLogLog, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("not enough data to read precision")
	}

	p := data[0]
	m := uint64(1) << p
	expectedLength := 1 + m
	if uint64(len(data)) < expectedLength {
		return nil, fmt.Errorf("not enough data to read register values: expected %d bytes, got %d", expectedLength, len(data))
	}

	register := make([]uint8, m)
	copy(register, data[1:1+m])

	return &HyperLogLog{
		p:        p,
		m:        m,
		register: register,
	}, nil
}
