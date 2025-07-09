package ssTable

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"os"
	"strings"

	"github.com/Vujovic0/NASP2024/config"

	"github.com/Vujovic0/NASP2024/blockManager"
)

// Used in making index and summary segments
type Tracker struct {
	dataTracker    *DataTracker
	indexTracker   *IndexTracker
	summaryTracker *SummaryTracker
}

type DataTracker struct {
	data   []byte  //only the data that will fit inside one block
	offset *uint64 //offset in the data file where to write new block
	file   *os.File
}

type IndexTracker struct {
	data             []byte  //all data that will be written to disk
	offset           *uint64 //offset in the index file where to write new block
	file             *os.File
	offsetStart      uint64 //at what block does index start (useful if compact)
	sparsity_counter uint64 //when sparsity_counter moduo global sparsity value equals 0, write to index data
}

type SummaryTracker struct {
	data             []byte   //all data that will be written to disk
	offset           *uint64  //offset in the summary file where to write new block
	file             *os.File //where to write data
	lastEntry        *Entry   //used as boundary
	offsetStart      uint64   //at what block does summary start (useful if compact)
	lastElementStart uint64   //at what block does last element start
	sparsity_counter uint64   //when sparsity_counter moduo global sparsity value equals 0, write to summary data
}

func initTracker() *Tracker {
	var tracker *Tracker = new(Tracker)
	tracker.dataTracker = new(DataTracker)
	tracker.dataTracker.offset = new(uint64)
	tracker.indexTracker = new(IndexTracker)
	tracker.indexTracker.offset = new(uint64)
	tracker.summaryTracker = new(SummaryTracker)
	tracker.summaryTracker.offset = new(uint64)
	return tracker
}

// | CRC 4B | TimeStamp 8B | Tombstone 1B | Keysize 8B | Valuesize 8B | Key... | Value... |
type Entry struct {
	crc       uint32
	timeStamp uint64
	tombstone bool
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
func InitEntry(crc uint32, tombstone bool, timeStamp uint64, key []byte, value []byte) *Entry {
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
func GetBlockEntries(file *os.File, offset uint64, limit uint64) ([]*Entry, uint64, error) {
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
		header := InitHeader(blockData[blockPointer:], true)
		crc := binary.LittleEndian.Uint32(blockData[blockPointer : blockPointer+4])
		timeStamp := GetTimeStamp(blockPointer, blockData, header)

		tombstone = GetTombstone(blockPointer, blockData, header)

		keySize := GetKeySize(blockPointer, blockData, header)
		keyStart := blockPointer + uint64(GetHeaderSize(header))
		keyEnd := keyStart + keySize
		if !tombstone {
			valueSize := GetValueSize(blockPointer, blockData, header, false, true)
			valEnd := keyEnd + valueSize
			key = blockData[keyStart:keyEnd]
			value = blockData[keyEnd:valEnd]
			blockPointer = valEnd
		} else {
			valueSize := uint64(0)
			key = blockData[keyStart:keyEnd]
			value = nil
			blockPointer = blockPointer + keyStart + keySize + valueSize
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
// Returns an entry that spans multiple data segment blocks as an array with a single entry pointer because
// arrays are used later in merge sorting
func getBlockEntriesTypeSplit(block *blockManager.Block, file *os.File, offset uint64, limit uint64) ([]*Entry, uint64, error) {
	blockData := block.GetData()[9:]

	var tombstone bool
	var key []byte
	var value []byte

	header := InitHeader(blockData, true)

	var dataBlock bool
	if strings.HasSuffix(block.GetFilePath(), "data.bin") || strings.HasSuffix(block.GetFilePath(), "compact.bin") {
		dataBlock = true
	} else {
		dataBlock = false
	}
	// first block
	crc := binary.LittleEndian.Uint32(blockData[0:4])
	timeStamp := GetTimeStamp(0, blockData, header)

	var valueSize uint64

	tombstone = GetTombstone(0, blockData, header)

	keySize := GetKeySize(0, blockData, header)

	headerSize := uint64(GetHeaderSize(header))

	if dataBlock {
		valueSize = GetValueSize(0, blockData, header, false, true)
	}

	if !tombstone {

		if len(blockData[headerSize:]) <= int(keySize) {
			key = append(key, blockData[headerSize:]...)
			keySize = keySize - uint64(len(blockData[headerSize:]))
		} else {
			key = append(key, blockData[headerSize:headerSize+keySize]...)
			value = append(value, blockData[headerSize+keySize:]...)
			if dataBlock {
				valueSize = valueSize - (uint64(len(blockData[headerSize+keySize:])))
			}
			keySize = 0
		}

	} else {
		key = append(key, blockData[headerSize:]...)
		keySize = keySize - uint64(len(blockData[headerSize:]))
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
				}
				value = append(value, blockData[keySize:]...)
				if dataBlock {
					valueSize = valueSize - (uint64(len(blockData)) - keySize)
				}
				keySize = 0
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
	if keySize != 0 {
		key = append(key, blockData[:keySize]...)
	}
	if !tombstone {
		if dataBlock {
			value = append(value, blockData[keySize:keySize+valueSize]...)
		} else {
			value = append(value, blockData[keySize:]...)
		}
		keySize = 0
	} else {
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
		fileSize := fileInfo.Size()
		lastBlockOffset := uint64(math.Ceil(float64(fileSize)/float64(config.GlobalBlockSize)) - 1)
		if strings.HasSuffix(fileName, "compact.bin") {

			lastBlock := blockManager.ReadBlock(file, lastBlockOffset)
			fileLimitBytes := lastBlock.GetData()[9+8 : 9+8*2]
			fileLimit := binary.LittleEndian.Uint64(fileLimitBytes)
			tableLimits[index] = fileLimit
		} else if strings.HasSuffix(fileName, "summary.bin") {

		} else {
			tableLimits[index] = lastBlockOffset + 1
		}
	}
	return tableLimits
}

// takes file pointers of tables that are compacting and the name of the new file for data
// newFilePath should have suffix "compact.bin" or "data.bin"
// !!! THE POINTERS MUST BE SORTED FROM OLDEST TO NEWEST !!!
// doesn't close the files when they reach the end
// doesn't delete the old tables
// if the compaction makes an empty table, delete the opened file (this is checked using lastElement)
// Folder where the new file should be needs to be created in advance
// File pointers need to be sorted from oldest to newest sstable
func MergeTables(filesArg []*os.File, newFilePath string) {
	files := make([]*os.File, len(filesArg))
	copy(files, filesArg)                                     //copies so the filesArg isn't updated while this function is running
	var entryArrays [][]*Entry = make([][]*Entry, len(files)) //entries for each iteration of block read are put here

	var tableLimits []uint64 = getLimits(files) //array of limits for all files
	var filesBlockOffsets []uint64 = make([]uint64, len(files))

	var tracker *Tracker = new(Tracker)
	tracker.dataTracker = new(DataTracker)
	tracker.indexTracker = new(IndexTracker)
	tracker.summaryTracker = new(SummaryTracker)

	var entryTableIndexMap map[*Entry]int = make(map[*Entry]int)
	var keyEntryMap map[string]*Entry = make(map[string]*Entry)
	entryHeap := &EntryHeap{}
	heap.Init(entryHeap)
	defineTracker(newFilePath, tracker)

	var err error

	//fills up heap and a map with entries and the index of their file
	for i := 0; i < len(entryArrays); i++ {
		if len(entryArrays[i]) == 0 {
			entryArrays[i], filesBlockOffsets[i], err = GetBlockEntries(files[i], filesBlockOffsets[i], tableLimits[i])
		}
		if err != nil {
			panic(err)
		}
		if len(entryArrays[i]) == 0 {
			continue
		}

		entry := entryArrays[i][0]
		keyStr := string(entry.key)
		if _, exists := keyEntryMap[keyStr]; exists {
			updateTableElement(
				tableLimits,
				files,
				entryTableIndexMap,
				keyEntryMap,
				entryHeap,
				entryArrays,
				filesBlockOffsets,
				i)
		} else {
			keyEntryMap[keyStr] = entry
			entryTableIndexMap[entry] = i
			heap.Push(entryHeap, keyStr)
		}
	}

	for entryHeap.Len() > 0 {
		minKey := heap.Pop(entryHeap).(string)
		minEntry := keyEntryMap[minKey]
		tableIndex := entryTableIndexMap[minEntry]
		entryArrays[tableIndex] = entryArrays[tableIndex][1:]
		delete(entryTableIndexMap, minEntry)
		delete(keyEntryMap, minKey)

		if !minEntry.tombstone {
			serializedEntry := SerializeEntry(minEntry, false)
			tracker.summaryTracker.lastEntry = minEntry
			flushDataIfFull(tracker, serializedEntry)
		}

		if len(entryArrays[tableIndex]) == 0 {
			entryArrays[tableIndex], filesBlockOffsets[tableIndex], err = GetBlockEntries(files[tableIndex], filesBlockOffsets[tableIndex], tableLimits[tableIndex])
			if err != nil {
				panic(err)
			}
		}

		if len(entryArrays[tableIndex]) == 0 {
			continue
		}

		entry := entryArrays[tableIndex][0]
		keyStr := string(entry.key)
		if _, exists := keyEntryMap[keyStr]; exists {
			updateTableElement(
				tableLimits,
				files,
				entryTableIndexMap,
				keyEntryMap,
				entryHeap,
				entryArrays,
				filesBlockOffsets,
				tableIndex)
		} else {
			keyEntryMap[keyStr] = entry
			entryTableIndexMap[entry] = tableIndex
			heap.Push(entryHeap, keyStr)
		}
	}

	//if there is no last entry, that means there are no valid entries at all so
	//the file should just get deleted
	if tracker.summaryTracker.lastEntry == nil {
		closeTracker(tracker)
		os.Remove(newFilePath)
		return
	}
	flushDataBytes(tracker.dataTracker.data, tracker)
	flushIndexBytes(tracker)
	flushSummaryBytes(tracker)
	getGeneration(true)
	closeTracker(tracker)
}

// called when there is an already existing key on the heap
func updateTableElement(
	tableLimits []uint64,
	files []*os.File, entryTableIndexMap map[*Entry]int,
	keyEntryMap map[string]*Entry,
	entryHeap *EntryHeap, entryArrays [][]*Entry,
	filesBlockOffsets []uint64,
	tableIndex int) {
	for {
		// if truncation reaches the last entry, try to load new entries
		if len(entryArrays[tableIndex]) == 0 {
			entryArrays[tableIndex], filesBlockOffsets[tableIndex], _ = GetBlockEntries(files[tableIndex], filesBlockOffsets[tableIndex], tableLimits[tableIndex])
		}
		// return if no more entries can be read
		if len(entryArrays[tableIndex]) == 0 {
			return
		}
		entryToAdd := entryArrays[tableIndex][0]

		//check if the key was already loaded onto heap
		if existingEntry, exists := keyEntryMap[string(entryToAdd.key)]; exists {
			//if entry is older, truncate entries
			if entryToAdd.timeStamp < existingEntry.timeStamp {
				entryArrays[tableIndex] = entryArrays[tableIndex][1:]
				continue
			}

			//if it is newer update table index for entry:index map and key:entry map
			keyEntryMap[string(entryToAdd.key)] = entryToAdd
			entryTableIndexMap[entryToAdd] = tableIndex

			//now we need load a new entry from the older sstable
			tableIndex = entryTableIndexMap[existingEntry]
			delete(entryTableIndexMap, existingEntry)
			continue
		}

		//add key to heap and update the entry:index map
		keyEntryMap[string(entryToAdd.key)] = entryToAdd
		entryTableIndexMap[entryToAdd] = tableIndex
		heap.Push(entryHeap, string(entryToAdd.key))
		return
	}
}

// Takes the tracker that tracks all data to write and offsets of their blocks as well as the
// files the data should be written to,
// and the data that is to be appended to tracker data
//
// If the serialized entry can't fit into array, empty out the array and then check if it can fit.
// If the array is already empty, but the entry can't fit, directly flush entry to disk.
//
// If the serialized entry can fit, just append.
func flushDataIfFull(tracker *Tracker, serializedEntry []byte) {
	dataArray := tracker.dataTracker.data
	blockHeaderSize := 9
	dataSpace := config.GlobalBlockSize - blockHeaderSize
	if len(dataArray)+len(serializedEntry) > dataSpace {
		if len(dataArray) == 0 {
			flushDataBytes(serializedEntry, tracker)
		} else {
			flushDataBytes(serializedEntry, tracker)
			dataArray = dataArray[:0]
			if len(serializedEntry) > config.GlobalBlockSize {
				flushDataBytes(serializedEntry, tracker)
			} else {
				dataArray = append(dataArray, serializedEntry...)
			}
		}
	} else {
		dataArray = append(dataArray, serializedEntry...)
	}
	tracker.dataTracker.data = dataArray //prilikom append operacije se pravi novi slice pa mora da se data pointer izmeni
}

// array is the serialized entry that is flushed to disk
// this function fills indexData of tracker if sparsity is correct and if the block
// is type full or type first which is checked with lastKey and newKey
func flushDataBytes(array []byte, tracker *Tracker) {
	file := tracker.dataTracker.file
	offset := tracker.dataTracker.offset
	var lastKey []byte = nil
	for channelResult := range PrepareSSTableBlocks(file.Name(), array, true, *offset, false) {
		block := channelResult.Block
		newKey := channelResult.Key
		blockManager.WriteBlock(file, block)
		if !bytes.Equal(lastKey, newKey) {
			lastKey = newKey
			if tracker.indexTracker.sparsity_counter%uint64(config.IndexSparsity) == 0 {
				if config.VariableEncoding {
					tracker.indexTracker.data = binary.AppendUvarint(tracker.indexTracker.data, uint64(len(newKey)))
				} else {
					tracker.indexTracker.data = binary.LittleEndian.AppendUint64(tracker.indexTracker.data, uint64(len(newKey)))
				}
				tracker.indexTracker.data = append(tracker.indexTracker.data, newKey...)
				if config.VariableEncoding {
					tracker.indexTracker.data = binary.AppendUvarint(tracker.indexTracker.data, uint64(*offset))
				} else {
					tracker.indexTracker.data = binary.LittleEndian.AppendUint64(tracker.indexTracker.data, uint64(*offset))
				}
			}
			tracker.indexTracker.sparsity_counter += 1
		}
		*offset += 1
	}
}

// | CRC 4B | TimeStamp 8B | Tombstone 1B | Keysize 8B | Valuesize 8B | Key... | Value... |
func SerializeEntry(entry *Entry, bound bool) []byte {
	if config.VariableEncoding {
		return serializeEntryCompressed(entry, bound)
	} else {
		return serializeEntryNonCompressed(entry, bound)
	}
}

func serializeEntryCompressed(entry *Entry, bound bool) []byte {
	serializedData := make([]byte, 0)
	if bound {
		serializedData = binary.AppendUvarint(serializedData, uint64(len(entry.key)))
		serializedData = append(serializedData, entry.key...)
		return serializedData
	}
	serializedData = binary.LittleEndian.AppendUint32(serializedData, entry.crc)
	serializedData = binary.AppendUvarint(serializedData, entry.timeStamp)
	if entry.tombstone {
		serializedData = append(serializedData, byte(1))
	} else {
		serializedData = append(serializedData, byte(0))
	}
	serializedData = binary.AppendUvarint(serializedData, uint64(len(entry.key)))
	serializedData = binary.AppendUvarint(serializedData, uint64(len(entry.value)))
	serializedData = append(serializedData, entry.key...)
	serializedData = append(serializedData, entry.value...)

	return serializedData
}

func serializeEntryNonCompressed(entry *Entry, bound bool) []byte {
	serializedData := make([]byte, 0)
	if bound {
		serializedData = binary.LittleEndian.AppendUint64(serializedData, uint64(len(entry.key)))
		serializedData = append(serializedData, entry.key...)
		return serializedData
	}
	serializedData = binary.LittleEndian.AppendUint32(serializedData, entry.crc)
	serializedData = binary.LittleEndian.AppendUint64(serializedData, entry.timeStamp)
	if entry.tombstone {
		serializedData = append(serializedData, 1)
	} else {
		serializedData = append(serializedData, 0)
	}
	serializedData = binary.LittleEndian.AppendUint64(serializedData, uint64(len(entry.key)))
	serializedData = binary.LittleEndian.AppendUint64(serializedData, uint64(len(entry.value)))
	serializedData = append(serializedData, entry.key...)
	serializedData = append(serializedData, entry.value...)
	return serializedData
}

// Adds pointers for correct file paths to tracker
func defineTracker(newFilePath string, tracker *Tracker) {
	if len(newFilePath) < 11 {
		panic("new file path is too short")
	}
	if !strings.HasSuffix(newFilePath, "compact.bin") && !strings.HasSuffix(newFilePath, "data.bin") {
		panic("new file path doesn't have correct suffix")
	}

	if strings.HasSuffix(newFilePath, "compact.bin") {
		defineTrackerCompact(newFilePath, tracker)
	} else if strings.HasSuffix(newFilePath, "data.bin") {
		defineTrackerSeparate(newFilePath, tracker)
	}
}

func closeTracker(tracker *Tracker) {
	tracker.indexTracker.file.Close()
	tracker.summaryTracker.file.Close()
	tracker.indexTracker.file.Close()
}

// Creates pointers for adequate files and pointers to adequate numbers
func defineTrackerSeparate(newFilePath string, tracker *Tracker) {
	filePrefix, found := strings.CutSuffix(newFilePath, "data.bin")
	if !found {
		panic("the suffix isn't data.bin")
	}
	newFileData, err := os.OpenFile(newFilePath, os.O_CREATE, 0664)
	if err != nil {
		panic(err)
	}
	newFileIndex, err := os.OpenFile(filePrefix+"index.bin", os.O_CREATE, 0664)
	if err != nil {
		panic(err)
	}
	newFileSummary, err := os.OpenFile(filePrefix+"summary.bin", os.O_CREATE, 0664)
	if err != nil {
		panic(err)
	}

	tracker.dataTracker.file = newFileData
	tracker.indexTracker.file = newFileIndex
	tracker.summaryTracker.file = newFileSummary

	tracker.dataTracker.offset = new(uint64)
	tracker.indexTracker.offset = new(uint64)
	tracker.summaryTracker.offset = new(uint64)
	*tracker.dataTracker.offset = 0
	*tracker.indexTracker.offset = 0
	*tracker.summaryTracker.offset = 0
}

// Makes all tracker file pointers point at the same file and indexes at the same number
func defineTrackerCompact(newFilePath string, tracker *Tracker) {
	newFile, err := os.OpenFile(newFilePath, os.O_CREATE, 0664)
	if err != nil {
		panic(err)
	}
	tracker.dataTracker.file = newFile
	tracker.indexTracker.file = newFile
	tracker.summaryTracker.file = newFile

	tracker.dataTracker.offset = new(uint64)
	*tracker.dataTracker.offset = 0
	tracker.indexTracker.offset = tracker.dataTracker.offset
	tracker.summaryTracker.offset = tracker.dataTracker.offset
}

func flushIndexBytes(tracker *Tracker) {
	array := tracker.indexTracker.data
	file := tracker.indexTracker.file
	offset := tracker.indexTracker.offset
	tracker.indexTracker.offsetStart = *offset
	var lastKey []byte = nil
	for channelResult := range PrepareSSTableBlocks(file.Name(), array, false, *offset, false) {
		block := channelResult.Block
		newKey := channelResult.Key
		blockManager.WriteBlock(file, block)
		if !bytes.Equal(lastKey, newKey) {
			lastKey = newKey
			if tracker.summaryTracker.sparsity_counter%uint64(config.SummarySparsity) == 0 {
				if config.VariableEncoding {
					tracker.summaryTracker.data = binary.AppendUvarint(tracker.summaryTracker.data, uint64(len(newKey)))
				} else {
					tracker.summaryTracker.data = binary.LittleEndian.AppendUint64(tracker.summaryTracker.data, uint64(len(newKey)))
				}
				tracker.summaryTracker.data = append(tracker.summaryTracker.data, newKey...)
				if config.VariableEncoding {
					tracker.summaryTracker.data = binary.AppendUvarint(tracker.summaryTracker.data, uint64(*offset))
				} else {
					tracker.summaryTracker.data = binary.LittleEndian.AppendUint64(tracker.summaryTracker.data, uint64(*offset))
				}
			}
			tracker.summaryTracker.sparsity_counter += 1
		}
		*offset += 1
	}
}

func flushSummaryBytes(tracker *Tracker) {
	array := tracker.summaryTracker.data
	file := tracker.summaryTracker.file
	offset := tracker.summaryTracker.offset
	tracker.summaryTracker.offsetStart = *offset
	for channelResult := range PrepareSSTableBlocks(file.Name(), array, false, *offset, false) {
		block := channelResult.Block
		blockManager.WriteBlock(file, block)
		*offset += 1
	}

	lastEntry := tracker.summaryTracker.lastEntry
	lastEntryData := SerializeEntry(lastEntry, true)

	tracker.summaryTracker.lastElementStart = *offset

	for channelResult := range PrepareSSTableBlocks(file.Name(), lastEntryData, false, *offset, true) {
		block := channelResult.Block
		blockManager.WriteBlock(file, block)
		*offset += 1
	}

	flushFooter(tracker)
}

func flushFooter(tracker *Tracker) {
	offset := tracker.summaryTracker.offset
	footerStart := *offset
	boundStart := tracker.summaryTracker.lastElementStart

	var footerData []byte = make([]byte, 0)

	footerData = binary.LittleEndian.AppendUint64(footerData, uint64(footerStart))
	if strings.HasSuffix(tracker.summaryTracker.file.Name(), "compact.bin") {
		footerData = binary.LittleEndian.AppendUint64(footerData, uint64(tracker.indexTracker.offsetStart))
		footerData = binary.LittleEndian.AppendUint64(footerData, uint64(tracker.summaryTracker.offsetStart))
	}

	footerData = binary.LittleEndian.AppendUint64(footerData, uint64(boundStart))

	var blockData []byte = make([]byte, config.GlobalBlockSize)
	copy(blockData[9:], footerData)
	blockData[4] = 0
	binary.LittleEndian.PutUint32(blockData[0:4], crc32.ChecksumIEEE(blockData[4:]))
	binary.LittleEndian.PutUint32(blockData[5:9], uint32(len(footerData)))

	block := blockManager.InitBlock(tracker.summaryTracker.file.Name(), *offset, blockData)
	blockManager.WriteBlock(tracker.summaryTracker.file, block)
}
