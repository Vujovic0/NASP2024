package ssTable

import (
	"encoding/binary"

	"github.com/Vujovic0/NASP2024/config"
)

// Header represent the number of bytes each header field takes up
type Header struct {
	crcBytes       int
	timeStampBytes int
	tombstoneBytes int
	keySizeBytes   int
	valueSizeBytes int
	dataBlock      bool
}

// Takes []byte where the first bytes should represent an entry header
// If compression is on, the header size of the entry is variable
// If compression is not on, the header size is fixed
// CRC and tombstone are always fixed to 4 bytes and 1 byte
// timestamp, keysize and valuesize are dynamic
func InitHeader(array []byte, dataBlock bool) *Header {
	header := new(Header)
	header.dataBlock = dataBlock
	headerPointer := 0
	if config.VariableEncoding {

		if dataBlock {
			header.crcBytes = 4
			headerPointer += header.crcBytes

			_, header.timeStampBytes = binary.Uvarint(array[headerPointer:])
			if header.timeStampBytes == 0 {
				panic("decoding failed")
			}
			headerPointer += header.timeStampBytes

			header.tombstoneBytes = 1
			headerPointer += header.tombstoneBytes
		}

		_, header.keySizeBytes = binary.Uvarint((array[headerPointer:]))
		if header.keySizeBytes == 0 {
			panic("decoding failed")
		}
		headerPointer += header.keySizeBytes

		if !dataBlock {
			header.valueSizeBytes = 0
		} else {
			if GetTombstone(0, array, header) {
				header.valueSizeBytes = 0
			} else {
				_, header.valueSizeBytes = binary.Uvarint((array[headerPointer:]))
				if header.valueSizeBytes == 0 {
					panic("decoding failed")
				}
			}
		}

	} else {
		if dataBlock {
			header.crcBytes = 4
			header.timeStampBytes = 8
			header.tombstoneBytes = 1
			if GetTombstone(0, array, header) {
				header.valueSizeBytes = 0
			} else {
				header.valueSizeBytes = 8
			}
		}
		header.keySizeBytes = 8
	}

	return header
}

func GetTimeStamp(dataPointer uint64, data []byte, header *Header) uint64 {
	offsetStart := dataPointer + uint64(header.crcBytes)
	offsetEnd := offsetStart + uint64(header.timeStampBytes)
	if !config.VariableEncoding {
		return binary.LittleEndian.Uint64(data[offsetStart:offsetEnd])
	} else {
		timestamp, _ := getUvarint(data[offsetStart:], "timestamp")
		return timestamp
	}
}

func GetTombstone(dataPointer uint64, data []byte, header *Header) bool {
	if header.tombstoneBytes == 0 {
		return false
	}
	offsetStart := dataPointer + uint64(header.crcBytes) + uint64(header.timeStampBytes)
	tombstone := byte(data[offsetStart])
	if tombstone == 1 {
		return true
	} else if tombstone == 0 {
		return false
	} else {
		panic("error reading tombstone, expected 1 or 0")
	}
}

func GetKeySize(dataPointer uint64, data []byte, header *Header) uint64 {
	offsetStart := dataPointer + uint64(header.crcBytes) + uint64(header.timeStampBytes) + uint64(header.tombstoneBytes)
	offsetEnd := offsetStart + uint64(header.keySizeBytes)
	if !config.VariableEncoding {
		return binary.LittleEndian.Uint64(data[offsetStart:offsetEnd])
	} else {
		keysize, _ := getUvarint(data[offsetStart:], "key size")
		return keysize
	}
}

// Gets valuesize assuming the entire data of an entry is passed
// Datapointer points to the first byte of the entry
func GetValueSize(dataPointer uint64, data []byte, header *Header, maxEntry bool, dataBlock bool) uint64 {
	if maxEntry {
		return 0
	}
	offsetStart := dataPointer + uint64(header.crcBytes) + uint64(header.timeStampBytes) + uint64(header.tombstoneBytes) + uint64(header.keySizeBytes)

	if !config.VariableEncoding {
		if !header.dataBlock {
			return 8
		}
		offsetEnd := offsetStart + uint64(header.valueSizeBytes)
		return binary.LittleEndian.Uint64(data[offsetStart:offsetEnd])
	} else {
		valuesize := uint64(0)
		if !dataBlock {
			_, n := getUvarint(data[offsetStart+GetKeySize(dataPointer, data, header):], "value size")
			valuesize = uint64(n)
		} else {
			tombstone := GetTombstone(dataPointer, data, header)
			if tombstone {
				return valuesize
			}
			valuesize, _ = getUvarint(data[offsetStart:], "value size")
		}
		return valuesize
	}
}

func getUvarint(buf []byte, field string) (uint64, int) {
	val, n := binary.Uvarint(buf)
	if n == 0 {
		panic("failed to decode: " + field)
	}
	return val, n
}

func GetHeaderSize(header *Header) int {
	return header.crcBytes + header.timeStampBytes + header.tombstoneBytes + header.keySizeBytes + header.valueSizeBytes
}
