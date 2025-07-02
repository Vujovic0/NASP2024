package ssTable

import (
	"errors"

	"github.com/Vujovic0/NASP2024/blockManager"
	"github.com/Vujovic0/NASP2024/config"
)

// Returns a byte slice of keys and their values of a full block.
//
// DataBlockCheck is true when reading from a data segment in sstable because entries
// in summary and index don't have CRC, timestamp and tombstone
//
// Summarybound is 0 when reading from non-summary segment.
// In another case it is an offset where the max element of the summary starts
//
// If value of an entry is an empty slice, it means that the tombstone is set to 1
func getKeysType0(block *blockManager.Block, dataBlockCheck bool, summaryBound uint64) ([][]byte, [][]byte, error) {
	firstSummaryBlock := false
	if summaryBound != 0 {
		if block.GetOffset() == summaryBound {
			firstSummaryBlock = true
		}
	}
	blockData := block.GetData()
	dataSize := uint64(block.GetSize())
	dataSize += 9

	var tombstone bool = false

	blockPointer := uint64(9)
	var keySlice [][]byte
	var valueSlice [][]byte
	var valueSize uint64

	for blockPointer < dataSize {
		header := InitHeader(blockData[blockPointer:], dataBlockCheck)

		if blockPointer+uint64(GetHeaderSize(header)) > dataSize {
			return nil, nil, errors.New("the block is corrupted: header bigger than data")
		}

		if dataBlockCheck {
			tombstone = GetTombstone(blockPointer, blockData, header)
		}

		keySize := GetKeySize(blockPointer, blockData, header)

		if tombstone {
			valueSize = 0
		} else {
			valueSize = GetValueSize(blockPointer, blockData, header, firstSummaryBlock, dataBlockCheck)
		}

		keyStart := blockPointer + uint64(GetHeaderSize(header))
		keyEnd := keyStart + keySize
		keyBytes := blockData[keyStart:keyEnd]

		valueBytes := make([]byte, 0)
		valueStart := keyEnd
		valueEnd := valueStart + valueSize
		if !firstSummaryBlock {
			valueBytes = blockData[valueStart:valueEnd]
		}

		keySlice = append(keySlice, keyBytes)
		valueSlice = append(valueSlice, valueBytes)
		tombstone = false
		firstSummaryBlock = false

		blockPointer += uint64(GetHeaderSize(header)) + keySize + valueSize
	}

	return keySlice, valueSlice, nil
}

// Returns key bytes, value bytes, key size left, value size left
func getKeysType1(block *blockManager.Block, dataBlockCheck bool, summaryBound uint64) ([]byte, []byte, uint64, uint64, error) {
	firstSummaryBlock := false
	if summaryBound != 0 {
		if block.GetOffset() == summaryBound {
			firstSummaryBlock = true
		}
	}
	blockData := block.GetData()
	dataSize := uint64(block.GetSize())
	dataSize += 9

	blockPointer := uint64(9)
	var keyBytes []byte = make([]byte, 0)
	var valueBytes []byte = make([]byte, 0)
	var valueSize uint64 = 0
	var tombstone bool = false

	header := InitHeader(blockData[9:], dataBlockCheck)
	if blockPointer+uint64(GetHeaderSize(header)) > dataSize {
		return nil, nil, 0, 0, errors.New("the block is corrupted: header bigger than data")
	}
	if dataBlockCheck {
		tombstone = GetTombstone(blockPointer, blockData, header)
		valueSize = GetValueSize(blockPointer, blockData, header, firstSummaryBlock, dataBlockCheck)
	}

	keySize := GetKeySize(blockPointer, blockData, header)

	headerSize := uint64(GetHeaderSize(header))
	//Check if key data fits inside or overflows further
	if blockPointer+headerSize+keySize > blockSize {
		keyBytes = append(keyBytes, blockData[blockPointer+headerSize:blockSize]...)
		keySize = keySize - (blockSize - blockPointer - headerSize)
		blockPointer = blockSize
	} else {
		keyBytes = append(keyBytes, blockData[blockPointer+headerSize:blockPointer+headerSize+keySize]...)
		blockPointer = blockPointer + headerSize + keySize
		keySize = 0
	}

	if !tombstone {
		//If the value data fits, then it should be type 0
		if dataBlockCheck && blockPointer+valueSize <= blockSize {
			return nil, nil, 0, 0, errors.New("the block is corrupted")
		}
		if !firstSummaryBlock {
			valueBytes = append(valueBytes, blockData[blockPointer:blockSize]...)
			if dataBlockCheck && !tombstone {
				valueSize = valueSize - (blockSize - blockPointer)
			}
		}

	}

	return keyBytes, valueBytes, keySize, valueSize, nil
}

// Returns key bytes, value bytes, key size left, value size left
func getKeysType2(block *blockManager.Block, keySize uint64, valueSize uint64, tombstone bool, dataBlock bool) ([]byte, []byte, uint64, uint64, error) {
	blockData := block.GetData()
	blockPointer := uint64(9)
	var keyBytes []byte = make([]byte, 0)
	var valueBytes []byte = make([]byte, 0)

	//Check if key data fits inside or overflows further
	if blockPointer+keySize > blockSize {
		keyBytes = append(keyBytes, blockData[blockPointer:blockSize]...)
		keySize -= blockSize - blockPointer
		blockPointer = blockSize
	} else {
		keyBytes = append(keyBytes, blockData[blockPointer:blockPointer+keySize]...)
		blockPointer += keySize
		keySize = 0
	}

	if !tombstone {
		//If the value data fits, then it should be type 3
		if !config.VariableEncoding && blockPointer+valueSize <= blockSize {
			return nil, nil, 0, 0, errors.New("the block is corrupted")
		}

		valueBytes = append(valueBytes, blockData[blockPointer:blockSize]...)
		if dataBlock && !tombstone {
			valueSize = valueSize - (blockSize - blockPointer)
		}
	}

	return keyBytes, valueBytes, keySize, valueSize, nil
}

// Returns key bytes, value bytes
func getKeysType3(block *blockManager.Block, keySize uint64, valueSize uint64, tombstone bool, dataBlock bool) ([]byte, []byte, error) {
	blockData := block.GetData()
	dataSize := uint64(block.GetSize())
	dataSize += 9
	blockPointer := uint64(9)
	var keyBytes []byte = make([]byte, 0)
	var valueBytes []byte = make([]byte, 0)

	//Check if key data fits inside or overflows further
	if !config.VariableEncoding && keySize+valueSize > dataSize {
		return nil, nil, errors.New("the block is corrupted")
	} else {
		keyBytes = append(keyBytes, blockData[blockPointer:blockPointer+keySize]...)
		blockPointer += keySize
	}

	if !tombstone {
		if dataBlock {
			valueBytes = append(valueBytes, blockData[blockPointer:blockPointer+valueSize]...)
		} else {
			valueBytes = append(valueBytes, blockData[blockPointer:]...)
		}
	}

	return keyBytes, valueBytes, nil
}
