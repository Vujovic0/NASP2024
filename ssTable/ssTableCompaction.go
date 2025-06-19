package ssTable

import (
	"NASP2024/blockManager"
	"NASP2024/config"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"slices"
)

// Used in making index and summary segments
type Tracker struct {
	dataData    [config.GlobalBlockSize]byte
	dataIndex   uint64
	dataFile    *os.File
	indexData   [config.GlobalBlockSize]byte
	indexIndex  uint64
	indexFile   *os.File
	summaryData [config.GlobalBlockSize]byte
	summaryFile *os.File ``
}

type Entry struct {
	crc       uint32
	tombstone bool
	timeStamp uint64
	key       []byte
	value     []byte
}

func initEntry(crc uint32, tombstone bool, timeStamp uint64, key []byte, value []byte) *Entry {
	return &Entry{
		crc:       crc,
		tombstone: tombstone,
		timeStamp: timeStamp,
		key:       key,
		value:     value,
	}
}

// Takes current block offset and compares it to the limit. Returns true if limit is same as offset and error if offstet > limit
//
// The limit should always be last offset + 1
func checkLimit(offset uint64, limit uint64) (error, bool) {
	if limit != 0 {
		if offset == limit {
			return nil, true
		} else if offset > limit {
			return errors.New("reading from index data not allowed"), false
		} else {
			return nil, false
		}
	}
	return errors.New("limit of data segment can't be 0"), false
}

// Takes filepath, block offset to read from and the last block offset of data segment
//
// Returns array of entry pointers, new block offset.
// Returns error if block type is other than 0 or 1.
func getBlockEntries(file *os.File, offset uint64, limit uint64) ([]*Entry, uint64, error) {
	err, endCheck := checkLimit(offset, limit)
	if err != nil {
		return nil, 0, err
	} else if endCheck {
		return nil, offset, nil
	}
	var entryArray []*Entry
	var block *blockManager.Block = blockManager.ReadBlock(file, offset)
	if block.GetType() == 0 {
		entryArray, err = getBlockEntriesTypeFull(block)
		offset += 1
	} else if block.GetType() == 1 {
		entryArray, offset, err = getBlockEntriesTypeSplit(block, file, offset, limit)
	} else {
		return nil, 0, errors.New("block type should be 0 or 1")
	}
	return entryArray, offset, err
}

// | CRC 4B | TimeStamp 8B | Tombstone 1B | Keysize 8B | Valuesize 8B | Key... | Value... |
//
// Returns all entries inside a block as an array of entry pointers
func getBlockEntriesTypeFull(block *blockManager.Block) ([]*Entry, error) {
	blockData := block.GetData()[9:]
	var blockPointer uint64 = 0

	var tombstone bool
	var key []byte
	var value []byte

	var entryArray []*Entry

	for blockPointer < uint64(block.GetSize()) {
		crc := binary.BigEndian.Uint32(blockData[blockPointer : blockPointer+4])
		timeStamp := binary.BigEndian.Uint64(blockData[blockPointer+4 : blockPointer+12])

		if blockData[blockPointer+12] == 1 {
			tombstone = true
		} else if blockData[blockPointer+12] == 0 {
			tombstone = false
		} else {
			return nil, errors.New("tombstone byte should be 1 or 0")
		}

		keySize := binary.BigEndian.Uint64(blockData[blockPointer+13 : blockPointer+21])
		if !tombstone {
			keyStart := blockPointer + 29
			keyEnd := keyStart + keySize
			valueSize := binary.BigEndian.Uint64(blockData[blockPointer+21 : blockPointer+29])
			valEnd := keyEnd + valueSize
			key = blockData[keyStart:keyEnd]
			value = blockData[keyEnd:valEnd]
			blockPointer = valEnd
		} else {
			valueSize := uint64(0)
			key = blockData[blockPointer+21 : blockPointer+21+keySize]
			value = nil
			blockPointer = blockPointer + keySize + valueSize + 21
		}

		entry := initEntry(crc, tombstone, timeStamp, key, value)
		entryArray = append(entryArray, entry)
	}

	if blockPointer != uint64(block.GetSize()) {
		return nil, errors.New("block is corrupted: blockPointer overshoots block size")
	}
	return entryArray, nil
}

// | CRC 4B | TimeStamp 8B | Tombstone 1B | Keysize 8B | Valuesize 8B | Key... | Value... |
// Returns an entry that spans multiple blocks as an array with a single entry pointer because
// arrays are used later in merge sorting
func getBlockEntriesTypeSplit(block *blockManager.Block, file *os.File, offset uint64, limit uint64) ([]*Entry, uint64, error) {
	blockData := block.GetData()[9:]

	var tombstone bool
	var key []byte
	var value []byte

	// first block
	crc := binary.BigEndian.Uint32(blockData[0:4])
	timeStamp := binary.BigEndian.Uint64(blockData[4:12])

	var valueSize uint64

	if blockData[12] == 1 {
		tombstone = true
	} else if blockData[12] == 0 {
		tombstone = false
	} else {
		return nil, 0, errors.New("tombstone byte should be 1 or 0")
	}

	keySize := binary.BigEndian.Uint64(blockData[13:21])

	if !tombstone {
		valueSize = binary.BigEndian.Uint64(blockData[21:29])
		if len(blockData[29:]) <= int(keySize) {
			key = append(key, blockData[29:]...)
			keySize = keySize - uint64(len(blockData[29:]))
		} else {
			key = append(key, blockData[29:29+keySize]...)
			keySize = 0
			value = append(value, blockData[29+keySize:]...)
			valueSize = valueSize - (uint64(len(blockData[29:])) - keySize)
		}

	} else {
		valueSize = uint64(0)
		key = append(key, blockData[21:]...)
		keySize = keySize - uint64(len(blockData[21:]))
	}

	offset += 1
	err, _ := checkLimit(offset, limit)
	if err != nil {
		return nil, 0, err
	}
	block = blockManager.ReadBlock(file, offset)
	// middle blocks
	for block.GetType() != 3 {
		blockData = block.GetData()[9:]
		if !tombstone {
			if len(blockData) <= int(keySize) {
				key = append(key, blockData...)
				keySize = keySize - uint64(len(blockData))
			} else {
				if keySize != 0 {
					key = append(key, blockData[:keySize]...)
					keySize = 0
				}
				value = append(value, blockData[keySize:]...)
				valueSize = valueSize - (uint64(len(blockData)) - keySize)
			}

		} else {
			key = append(key, blockData...)
			keySize = keySize - uint64(len(blockData))
		}
		offset += 1
		err, _ := checkLimit(offset, limit)
		if err != nil {
			return nil, 0, err
		}
		block = blockManager.ReadBlock(file, offset)
	}

	// last block
	blockData = block.GetData()[9:]
	if !tombstone {
		if len(blockData) <= int(keySize) {
			key = append(key, blockData...)
			keySize = keySize - uint64(len(blockData))
		} else {
			if keySize != 0 {
				key = append(key, blockData[:keySize]...)
				keySize = 0
			}
			value = append(value, blockData[keySize:]...)
			valueSize = valueSize - (uint64(len(blockData)) - keySize)
		}

	} else {
		key = append(key, blockData...)
		keySize = keySize - uint64(len(blockData))
	}
	offset += 1

	entry := initEntry(crc, tombstone, timeStamp, key, value)
	return []*Entry{entry}, offset, nil
}

// Goes through all files that need to merge. If it is a compact type, the last block containing all
// limits will be read. The first 9 bytes are block header, and 8 bytes after are representing the start
// of the index segment (aka the end of data segment).
// If the file is not compact, the limit is the index of the last block + 1.
func getLimits(files []*os.File) []uint64 {
	var tableLimits []uint64 = make([]uint64, len(files))
	for index, file := range files {
		fileName := file.Name()
		fileInfo, _ := file.Stat()
		lastBlockOffset := uint64(fileInfo.Size())/uint64(config.GlobalBlockSize) - 1
		if fileName[len(fileName)-11:] == "compact.bin" {

			lastBlock := blockManager.ReadBlock(file, lastBlockOffset)
			fileLimitBytes := lastBlock.GetData()[9 : 9+8]
			fileLimit := binary.BigEndian.Uint64(fileLimitBytes)
			tableLimits[index] = fileLimit
		} else {
			tableLimits[index] = lastBlockOffset + 1
		}
	}
	return tableLimits
}

// takes file pointers of tables that are compacting and the name of the new file
// doesn't close the files when they reach the end
// doesn't delete the old tables
func MergeTables(filesArg []*os.File, newFilePath string) {
	files := make([]*os.File, len(filesArg))
	copy(files, filesArg)
	var entryArrays [][]*Entry = make([][]*Entry, len(files)) //entries for each iteration of block read are put here
	var minimumEntry *Entry
	var minimumIndex int                        //index of the file with the current smallest entry
	var tableLimits []uint64 = getLimits(files) //array of limits for all files
	var filesBlockOffsets []uint64 = make([]uint64, len(files))
	var dataToWrite []byte //data that will be written after reaching a byte threshold (block size)
	var newFileBlockOffset uint64
	var tracker *Tracker
	var counter int = 0 // tracks the current file in the array of files it should compare minimum entry with

	newFile, err := os.OpenFile(newFilePath, os.O_CREATE, 0664)
	if err != nil {
		panic(err)
	}
	defer newFile.Close()

	//iterates through all file entries until there are no files to read
	for len(entryArrays) != 0 {
		//finds the smallest entry
		for counter < len(entryArrays) {
			//try to fill up entryArrays, if failed pop at index
			if len(entryArrays[counter]) == 0 {
				entryArrays[counter], filesBlockOffsets[counter], err = getBlockEntries(files[counter], uint64(filesBlockOffsets[counter]), tableLimits[counter])

				if err != nil {
					panic(err)
				}
				if len(entryArrays[counter]) == 0 {
					entryArrays = slices.Delete(entryArrays, counter, counter+1)
					filesBlockOffsets = slices.Delete(filesBlockOffsets, counter, counter+1)
					files = slices.Delete(files, counter, counter+1)
					tableLimits = slices.Delete(tableLimits, counter, counter+1)
					continue
				}
			}
			//takes first entry as smallest because the smallest of all files will always be popped
			entryIteration := entryArrays[counter][0]

			//compares current smallest to the overall smallest
			if minimumEntry == nil {
				minimumEntry = entryIteration
				minimumIndex = counter
			} else {
				comparisonResult := bytes.Compare(minimumEntry.key, entryIteration.key)
				if comparisonResult > 0 {
					minimumEntry = entryIteration
					minimumIndex = counter
				} else if comparisonResult == 0 {
					if minimumEntry.timeStamp > entryIteration.timeStamp {
						minimumEntry = entryIteration
						minimumIndex = counter
					}
				}
			}
			counter += 1
		}
		if minimumEntry != nil {
			if !minimumEntry.tombstone {
				serializedEntry := SerializeEntry(minimumEntry)
				flushIfFull(tracker, serializedEntry)
			}
			//pops first element of the slice in O(1) time because slices are cool
			//the first element represents the current smallest key in all tables
			entryArrays[minimumIndex] = entryArrays[minimumIndex][1:]
			minimumEntry = nil
			counter = 0
		}

	}
}

// Takes the tracker that tracks all data to write and offsets of their blocks as well as the
// files the data should be written to,
// and the data that is to be appended to tracker data
//
// If the serialized entry can't fit into array, empty out the array and then check if it can fit.
// If it still can't fit flush it directly onto the disk.
// If the array is already empty, directly flush entry to disk.
// If the serialized entry can fit, just append.
func flushIfFull(tracker *Tracker, serializedEntry []byte) {
	dataArray := tracker.dataData
	dataFile := files[0]
	if len(dataArray)+len(serializedEntry) > config.GlobalBlockSize {
		if len(dataArray) == 0 {
			flushBytes(serializedEntry, offset, file)
		} else {
			offset = flushBytes(dataArray, offset, file)
			dataArray = dataArray[:0]
			if len(serializedEntry) > config.GlobalBlockSize {
				offset = flushBytes(serializedEntry, offset, file)
			} else {
				dataArray = append(dataArray, serializedEntry...)
			}
		}
	} else {
		dataArray = append(dataArray, serializedEntry...)
	}
}

func flushBytes(array []byte, offset uint64, file *os.File) uint64 {
	for channelResult := range PrepareSSTableBlocks(file.Name(), array, true, offset, false) {
		blockManager.WriteBlock(file, channelResult.Block)
		offset += 1
	}
	return offset
}

// | CRC 4B | TimeStamp 8B | Tombstone 1B | Keysize 8B | Valuesize 8B | Key... | Value... |
func SerializeEntry(entry *Entry) []byte {
	headerSize := 0

	if entry.tombstone {
		headerSize = 21
	} else {
		headerSize = 29
	}

	data := make([]byte, headerSize+len(entry.key)+len(entry.value))

	binary.BigEndian.PutUint64(data[4:12], entry.timeStamp)

	copy(data[headerSize:], entry.key)

	binary.BigEndian.PutUint64(data[13:21], uint64(len(entry.key)))
	if !entry.tombstone {
		data[12] = 0
		binary.BigEndian.PutUint64(data[21:29], uint64(len(entry.value)))
		copy(data[headerSize+len(entry.key):], entry.value)
	} else {
		data[12] = 1
	}

	binary.BigEndian.PutUint32(data[0:4], crc32.ChecksumIEEE(data[4:]))

	return data
}
