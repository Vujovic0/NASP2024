package ssTable

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
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

type Footer struct {
	MaxKeyOffset uint64
}

type FileTracker struct {
	DataFile   *os.File // fajl iz kog čitaš blokove
	DataBuffer []byte   // alternativa ako koristiš mmap ili ucitano u RAM
	Footer     Footer   // sadrži MaxKeyOffset itd.
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
	path             string
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
	Key       []byte
	value     []byte
}

func initEntry(crc uint32, tombstone bool, timeStamp uint64, key []byte, value []byte) *Entry {
	return &Entry{
		crc:       crc,
		tombstone: tombstone,
		timeStamp: timeStamp,
		Key:       key,
		value:     value,
	}
}

func InitEntry(crc uint32, tombstone bool, timeStamp uint64, key []byte, value []byte) *Entry {
	return &Entry{
		crc:       crc,
		tombstone: tombstone,
		timeStamp: timeStamp,
		Key:       key,
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
// Returns array of entry pointers, new block offset.
// Returns error if block type is other than 0 or 1.
func getBlockEntries(file *os.File, offset uint64, limit uint64) ([]*Entry, uint64, error) {
	if offset >= limit {
		return nil, offset, nil
	}

	var entryArray []*Entry

	block := blockManager.ReadBlock(file, offset)
	if block == nil {
		return nil, offset, fmt.Errorf("failed to read block at offset %d", offset)
	}

	blockType := block.GetType()
	var err error
	switch blockType {
	case 0:
		entryArray, err = getBlockEntriesTypeFull(block)
		offset += 1
	case 1:
		entryArray, offset, err = getBlockEntriesTypeSplit(block, file, offset, limit)
	default:
		return nil, offset, fmt.Errorf("unknown block type %d", blockType)
	}

	if err != nil {
		return nil, offset, err
	}

	if len(entryArray) == 0 {
		return nil, offset, nil
	}

	for _, e := range entryArray {
		if e == nil || len(e.Key) == 0 {
			return nil, offset, fmt.Errorf("invalid entry in block at offset %d", offset)
		}
	}

	return entryArray, offset, nil
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
	fmt.Println("Merging tables into:", newFilePath)

	files := make([]*os.File, len(filesArg))
	copy(files, filesArg)

	entryArrays := make([][]*Entry, len(files))
	tableLimits := getLimits(files)
	filesBlockOffsets := make([]uint64, len(files))

	tracker := initTracker()
	defineTracker(newFilePath, tracker)

	entryTableIndexMap := make(map[*Entry]int)
	keyEntryMap := make(map[string]*Entry)
	entryHeap := &EntryHeap{}
	heap.Init(entryHeap)

	for i := 0; i < len(files); i++ {
		var err error
		entryArrays[i], filesBlockOffsets[i], err = getBlockEntries(files[i], filesBlockOffsets[i], tableLimits[i])
		if err != nil || len(entryArrays[i]) == 0 || entryArrays[i][0] == nil {
			continue
		}
		entry := entryArrays[i][0]
		keyStr := string(entry.Key)
		if keyStr == "" {
			continue
		}
		keyEntryMap[keyStr] = entry
		entryTableIndexMap[entry] = i
		heap.Push(entryHeap, entry)
	}

	for entryHeap.Len() > 0 {
		minEntry := heap.Pop(entryHeap).(*Entry)
		tableIndex := entryTableIndexMap[minEntry]
		delete(entryTableIndexMap, minEntry)
		delete(keyEntryMap, string(minEntry.Key))

		if !minEntry.tombstone {
			serialized := SerializeEntry(minEntry, false)
			tracker.summaryTracker.lastEntry = minEntry
			flushDataIfFull(tracker, serialized)
		}

		entryArrays[tableIndex] = entryArrays[tableIndex][1:]
		if len(entryArrays[tableIndex]) == 0 {
			var err error
			entryArrays[tableIndex], filesBlockOffsets[tableIndex], err = getBlockEntries(files[tableIndex], filesBlockOffsets[tableIndex], tableLimits[tableIndex])
			if err != nil || len(entryArrays[tableIndex]) == 0 || entryArrays[tableIndex][0] == nil {
				continue
			}
		}

		entry := entryArrays[tableIndex][0]
		keyStr := string(entry.Key)
		if _, exists := keyEntryMap[keyStr]; exists {
			updateTableElement(tableLimits, files, entryTableIndexMap, keyEntryMap, entryHeap, entryArrays, filesBlockOffsets, tableIndex)
		} else {
			keyEntryMap[keyStr] = entry
			entryTableIndexMap[entry] = tableIndex
			heap.Push(entryHeap, entry)
		}
	}

	if tracker.summaryTracker.lastEntry == nil {
		closeTracker(tracker)
		os.Remove(newFilePath)
		return
	}

	if strings.HasSuffix(newFilePath, "-compact.bin") {
		flushDataBytes(tracker.dataTracker.data, tracker)
		flushIndexBytes(tracker)
		flushSummaryBytes(tracker)
		err := WriteFooter(tracker.dataTracker.file, *tracker.indexTracker.offset, *tracker.summaryTracker.offset, 0)
		if err != nil {
			panic(err)
		}
		tracker.dataTracker.file.Sync()
		tracker.indexTracker.file.Sync()
		tracker.summaryTracker.file.Sync()
	} else if strings.HasSuffix(newFilePath, "-data.bin") {
		flushDataBytes(tracker.dataTracker.data, tracker)
		flushIndexBytes(tracker)
		flushSummaryBytes(tracker)
		block := blockManager.NewBlock(3)
		block.Add(SerializeEntry(tracker.summaryTracker.lastEntry, true))
		blockManager.WriteBlock(tracker.summaryTracker.file, block)
		err := binary.Write(tracker.summaryTracker.file, binary.LittleEndian, block.GetOffset())
		if err != nil {
			panic(err)
		}
		tracker.dataTracker.file.Sync()
		tracker.indexTracker.file.Sync()
		tracker.summaryTracker.file.Sync()
	} else {
		panic("Invalid file suffix: must end in -compact.bin or -data.bin")
	}

	closeTracker(tracker)

	// Debug
	if info, err := os.Stat(tracker.dataTracker.file.Name()); err == nil {
		fmt.Println("Final merged SSTable file size:", info.Size())
	}

	getGeneration(true)
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
			entryArrays[tableIndex], filesBlockOffsets[tableIndex], _ = getBlockEntries(files[tableIndex], filesBlockOffsets[tableIndex], tableLimits[tableIndex])
		}
		// return if no more entries can be read
		if len(entryArrays[tableIndex]) == 0 {
			return
		}
		entryToAdd := entryArrays[tableIndex][0]

		//check if the key was already loaded onto heap
		if existingEntry, exists := keyEntryMap[string(entryToAdd.Key)]; exists {
			//if entry is older, truncate entries
			if entryToAdd.timeStamp < existingEntry.timeStamp {
				if len(entryArrays[tableIndex]) > 0 {
					entryArrays[tableIndex] = entryArrays[tableIndex][1:]
				} else {
					// Try to read new blocks from file
					entryArrays[tableIndex], filesBlockOffsets[tableIndex], _ = getBlockEntries(files[tableIndex], filesBlockOffsets[tableIndex], tableLimits[tableIndex])
					if len(entryArrays[tableIndex]) == 0 {
						return // no more data
					}
				}
				continue
			} else if entryToAdd.timeStamp == existingEntry.timeStamp {
				if tableIndex < entryTableIndexMap[existingEntry] {
					// prefer entry from newer file if timestamps equal
				} else {
					entryArrays[tableIndex] = entryArrays[tableIndex][1:]
					continue
				}
			}

			//if it is newer update table index for entry:index map and key:entry map
			keyEntryMap[string(entryToAdd.Key)] = entryToAdd
			entryTableIndexMap[entryToAdd] = tableIndex

			entryArrays[tableIndex] = entryArrays[tableIndex][1:]

			tableIndex = entryTableIndexMap[existingEntry]
			delete(entryTableIndexMap, existingEntry)
			continue

		}

		//add key to heap and update the entry:index map
		keyEntryMap[string(entryToAdd.Key)] = entryToAdd
		entryTableIndexMap[entryToAdd] = tableIndex
		heap.Push(entryHeap, entryToAdd)
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
		serializedData = binary.AppendUvarint(serializedData, uint64(len(entry.Key)))
		serializedData = append(serializedData, entry.Key...)
		return serializedData
	}
	serializedData = binary.LittleEndian.AppendUint32(serializedData, entry.crc)
	serializedData = binary.AppendUvarint(serializedData, entry.timeStamp)
	if entry.tombstone {
		serializedData = append(serializedData, byte(1))
	} else {
		serializedData = append(serializedData, byte(0))
	}
	serializedData = binary.AppendUvarint(serializedData, uint64(len(entry.Key)))
	serializedData = binary.AppendUvarint(serializedData, uint64(len(entry.value)))
	serializedData = append(serializedData, entry.Key...)
	serializedData = append(serializedData, entry.value...)

	return serializedData
}

func serializeEntryNonCompressed(entry *Entry, bound bool) []byte {
	serializedData := make([]byte, 0)
	if bound {
		serializedData = binary.LittleEndian.AppendUint64(serializedData, uint64(len(entry.Key)))
		serializedData = append(serializedData, entry.Key...)
		return serializedData
	}
	serializedData = binary.LittleEndian.AppendUint32(serializedData, entry.crc)
	serializedData = binary.LittleEndian.AppendUint64(serializedData, entry.timeStamp)
	if entry.tombstone {
		serializedData = append(serializedData, 1)
	} else {
		serializedData = append(serializedData, 0)
	}
	serializedData = binary.LittleEndian.AppendUint64(serializedData, uint64(len(entry.Key)))
	serializedData = binary.LittleEndian.AppendUint64(serializedData, uint64(len(entry.value)))
	serializedData = append(serializedData, entry.Key...)
	serializedData = append(serializedData, entry.value...)
	return serializedData
}

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

// U kompakt fajlu imamo:
// summary = compact.bin fajl
// data = data.bin fajl (isti prefix)
// index = index.bin fajl (isti prefix)
func defineTrackerCompact(newFilePath string, tracker *Tracker) {
	basePath := strings.TrimSuffix(newFilePath, "-compact.bin")
	dataPath := basePath + "-data.bin"
	indexPath := basePath + "-index.bin"
	summaryPath := newFilePath

	var err error
	tracker.dataTracker.file, err = os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	tracker.indexTracker.file, err = os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	tracker.summaryTracker.file, err = os.OpenFile(summaryPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}

	tracker.summaryTracker.path = summaryPath

	*tracker.dataTracker.offset = 0
	*tracker.indexTracker.offset = 0
	*tracker.summaryTracker.offset = 0
}

// Odvojeni fajlovi sa sufiksima: data.bin, index.bin, summary.bin
func defineTrackerSeparate(newFilePath string, tracker *Tracker) {
	// newFilePath je sa suffixom data.bin, npr: "usertable-3-data.bin"
	basePath := strings.TrimSuffix(newFilePath, "data.bin")

	dataPath := basePath + "data.bin"
	indexPath := basePath + "index.bin"
	summaryPath := basePath + "summary.bin"

	var err error
	tracker.dataTracker.file, err = os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	tracker.indexTracker.file, err = os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	tracker.summaryTracker.file, err = os.OpenFile(summaryPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}

	tracker.summaryTracker.path = summaryPath

	*tracker.dataTracker.offset = 0
	*tracker.indexTracker.offset = 0
	*tracker.summaryTracker.offset = 0
}

func closeTracker(tracker *Tracker) {
	if tracker.dataTracker.file != nil {
		tracker.dataTracker.file.Close()
	}
	if tracker.indexTracker.file != nil {
		tracker.indexTracker.file.Close()
	}
	if tracker.summaryTracker.file != nil {
		tracker.summaryTracker.file.Close()
	}
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

func flushSummaryBytes(tracker *Tracker) {
	var err error

	summaryPath := tracker.summaryTracker.path
	file := tracker.summaryTracker.file
	if file == nil {
		file, err = os.OpenFile(summaryPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			panic("cannot open summary file for writing: " + err.Error())
		}
		tracker.summaryTracker.file = file
	}

	offset := tracker.summaryTracker.offset
	tracker.summaryTracker.offsetStart = *offset

	for channelResult := range PrepareSSTableBlocks(file.Name(), tracker.summaryTracker.data, false, *offset, false) {
		block := channelResult.Block
		blockManager.WriteBlock(file, block)
		*offset += 1
	}

	lastEntry := tracker.summaryTracker.lastEntry
	if lastEntry != nil {
		lastEntryData := SerializeEntry(lastEntry, true)
		tracker.summaryTracker.lastElementStart = *offset

		for channelResult := range PrepareSSTableBlocks(file.Name(), lastEntryData, false, *offset, true) {
			block := channelResult.Block
			blockManager.WriteBlock(file, block)
			*offset += 1
		}
	}

	flushFooter(tracker)

	if err := file.Sync(); err != nil {
		panic("sync summary file failed: " + err.Error())
	}
	if err := file.Close(); err != nil {
		panic("close summary file failed: " + err.Error())
	}
	tracker.summaryTracker.file = nil
}
