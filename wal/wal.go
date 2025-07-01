package wal

import (
	"NASP2024/blockManager"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const BlockHeaderSize = 9

type LogEntry struct {
	Timestamp time.Time
	Tombstone bool
	Key       string
	Value     string
}

type WAL struct {
	walNames         []string // NAMES OF ALL WALx.LOG
	blockSize        int      //
	segmentSize      int      // HOW MANY BLOCKS WE HAVE IN ONE FILE
	currentFile      *os.File // POINTER TO A FILE THAT WE ARE CURRENTLY USING
	currentBlock     int      // OFFSET WHERE WE WRITE
	freeBlock        int      // HOW MUCH FREE SPACE WE GOT IN CURRENT BLOCK THAT WE WRITE IN
	minimumEntrySize int      // HELPER ATTRIBUTE FOR PADDING
}

func NewWAL(blockSize int, segmentSize int, currentBlock int, freeBlock int) *WAL {
	wal := new(WAL)

	file, err1 := os.OpenFile("./wal/wals", os.O_RDONLY, 066)
	if err1 != nil {
		panic(err1)
	}
	namesOfWals, err2 := file.Readdirnames(0)
	if err2 != nil {
		panic(err2)
	}
	file.Close()
	if len(namesOfWals) == 0 {
		file, err := os.Create("./wal/wals/wal0.bin")
		if err != nil {
			fmt.Println("Failed to create the first WAL file:", err)
		}

		// Update WAL state
		wal.currentFile = file
		wal.walNames = append(wal.walNames, "wal0.bin")
		wal.currentBlock = 0
	}
	//fileIndex, blockNumber, logOffset := LoadOffset()

	// DODAJ DA SE PROCITANI OFFSET UBACUJE U WAL
	wal.walNames = namesOfWals
	wal.segmentSize = segmentSize
	wal.blockSize = blockSize
	wal.currentBlock = currentBlock
	wal.freeBlock = freeBlock
	//wal.currentWalName = namesOfWals[len(namesOfWals)-1]
	// DODAJ DA JE CURRENTWAL POKAZIVAČ NA OS.FILE
	wal.minimumEntrySize = 35
	return wal
}

func LoadOffset() (fileIndex, blockNumber, logOffset uint32) {
	file, err := os.Open("wal/offset.bin")
	if err != nil {
		if os.IsNotExist(err) {
			// IF FILE DOESN'T EXIST, WE CREATE NEW WITH WITH 0, 0, 0 VALUES
			emptyValues := make([]byte, 12)
			binary.LittleEndian.PutUint32(emptyValues[0:4], 0)  // file index
			binary.LittleEndian.PutUint32(emptyValues[4:8], 0)  // block number
			binary.LittleEndian.PutUint32(emptyValues[8:12], 0) // log offset

			err := os.WriteFile("wal/offset.bin", emptyValues, 0644)
			if err != nil {
				panic("Failed to create offset.bin: " + err.Error())
			}
			return 0, 0, 0
		} else {
			panic("Failed to open offset.bin: " + err.Error())
		}
	}
	defer file.Close()

	block := blockManager.ReadBlock(file, 0)
	blockData := block.GetData()[9:] // [9:] SO WE CAN SKIP BLOCK HEADER

	fileIndex = binary.LittleEndian.Uint32(blockData[0:4])
	blockNumber = binary.LittleEndian.Uint32(blockData[4:8])
	logOffset = binary.LittleEndian.Uint32(blockData[8:12])
	return
}

func NewLogEntry(key string, value string) *LogEntry {
	logEntry := new(LogEntry)
	logEntry.Key = key
	logEntry.Value = value
	logEntry.Tombstone = false
	logEntry.Timestamp = time.Now()
	return logEntry
}

func (wal *WAL) WriteLogEntry(key string, value string) {
	log := NewLogEntry(key, value)
	logEntryBytes := log.SerializeLogEntry()
	logSizeNeeded := log.GetSerializedLogSize()
	if logSizeNeeded+BlockHeaderSize > wal.blockSize {
		wal.WriteLogEntryType0(logEntryBytes, logSizeNeeded)
	} else {
		wal.WriteLongEntryOtherType(logEntryBytes, logSizeNeeded)
	}
}

func (wal *WAL) WriteLongEntryOtherType(logEntryBytes []byte, logSizeNeeded int) {
	wal.currentBlock += 1
	if wal.currentBlock == wal.segmentSize {
		wal.InitNewFile()
	}

	// HELPER INTS TO TRACK WHICH PARTS OF LOG HAVE WE WRITTEN
	startOffset := 0
	stopOffset := wal.blockSize - BlockHeaderSize

	blockType := make([]byte, 1)
	blockType[0] = 1
	// WRITING THE FIRST TYPE 1 BLOCK
	blockData := wal.BuildBlock(blockType, logEntryBytes[startOffset:stopOffset])
	block := blockManager.InitBlock(wal.currentFile.Name(), uint64(wal.currentBlock), blockData)
	blockManager.WriteBlock(wal.currentFile, block)
	wal.currentFile.Sync()
	// WRITING TYPE 2 BLOCKS UNTIL THE LAST PART OF LOG CAN FIT INTO A SINGLE BLOCK
	blockType[0] = 2
	for {
		startOffset = stopOffset
		stopOffset += wal.blockSize - BlockHeaderSize
		if (logSizeNeeded - stopOffset) <= (wal.blockSize - BlockHeaderSize) {
			break
		}
		wal.currentBlock += 1
		if wal.currentBlock == wal.segmentSize {
			wal.InitNewFile()
		}
		blockData := wal.BuildBlock(blockType, logEntryBytes[startOffset:stopOffset])
		block := blockManager.InitBlock(wal.currentFile.Name(), uint64(wal.currentBlock), blockData)
		blockManager.WriteBlock(wal.currentFile, block)
		wal.currentFile.Sync()
	}
	// WHEN WE FINISH WITH ALL TYPE 2 BLOCKS, WE WRITE FINAL TYPE 3 BLOCK
	wal.currentBlock += 1
	if wal.currentBlock == wal.segmentSize {
		wal.InitNewFile()
	}
	blockType[0] = 3
	blockData = wal.BuildBlock(blockType, logEntryBytes[startOffset:])
	block = blockManager.InitBlock(wal.currentFile.Name(), uint64(wal.currentBlock), blockData)
	blockManager.WriteBlock(wal.currentFile, block)
	wal.currentFile.Sync()
	wal.freeBlock = wal.blockSize - 9 - len(logEntryBytes[startOffset:])
}

func (wal *WAL) WriteLogEntryType0(logEntryBytes []byte, logSizeNeeded int) {
	currentBlock := blockManager.ReadBlock(wal.currentFile, uint64(wal.currentBlock))

	if currentBlock.GetType() == 0 { // IF CURRENT BLOCK IS FILLED WITH THE END OF THE OLDER LOG
		if wal.freeBlock >= logSizeNeeded {
			// WRITING IN THE SAME BLOCK
			data := append(currentBlock.GetData()[BlockHeaderSize:], logEntryBytes...)
			blockType := make([]byte, 1)
			blockType[0] = 0
			newBlockBytes := wal.BuildBlock(blockType, data)
			wal.freeBlock = wal.blockSize - len(newBlockBytes)
			block := blockManager.InitBlock(wal.currentFile.Name(), uint64(wal.currentBlock), newBlockBytes)
			blockManager.WriteBlock(wal.currentFile, block)
			wal.currentFile.Sync()
		} else {
			wal.currentBlock += 1
			if wal.currentBlock == wal.segmentSize {
				// WRITING IN NEW WAL FILE
				wal.InitNewFile()
				blockType := make([]byte, 1)
				blockType[0] = 0
				newBlockBytes := wal.BuildBlock(blockType, logEntryBytes)
				block := blockManager.InitBlock(wal.currentFile.Name(), uint64(wal.currentBlock), newBlockBytes)
				blockManager.WriteBlock(wal.currentFile, block)
				wal.currentFile.Sync()
				wal.freeBlock = wal.blockSize - len(newBlockBytes)
			} else {
				// WRITING IN NEW BLOCK IN SAME WAL FILE
				blockType := make([]byte, 1)
				blockType[0] = 0
				blockData := wal.BuildBlock(blockType, logEntryBytes)
				block := blockManager.InitBlock(wal.currentFile.Name(), uint64(wal.currentBlock), blockData)
				blockManager.WriteBlock(wal.currentFile, block)
				wal.currentFile.Sync()
				wal.freeBlock = wal.blockSize - len(blockData)
			}
		}
	}
}

func (wal *WAL) BuildBlock(blockType []byte, blockData []byte) []byte {
	dataSize := uint32(len(blockData))
	block := make([]byte, wal.blockSize)
	block[4] = blockType[0]
	binary.BigEndian.PutUint32(block[5:BlockHeaderSize], dataSize)
	copy(block[9:], blockData)
	crc := crc32.ChecksumIEEE(block[4:])
	binary.BigEndian.PutUint32(block[0:4], crc)
	return block
}

func (wal *WAL) InitNewFile() bool {
	wal.currentBlock = 0 // RESETTING BLOCK INDEX FOR THE NEW FILE

	// Close the current file safely
	if wal.currentFile != nil {
		wal.currentFile.Close()
	}

	// Get last used WAL number
	base := strings.TrimSuffix(wal.walNames[len(wal.walNames)-1], ".bin") // CUTTING OFF .BIN
	numberStr := strings.TrimPrefix(base, "wal")                          // CUTTING OFF "wal"
	number, err := strconv.Atoi(numberStr)                                // CONVERTING TO INT
	if err != nil {
		fmt.Println("Failed to parse current WAL number:", err)
		return false
	}

	newFileName := fmt.Sprintf("wal%d.bin", number+1)
	fullPath := filepath.Join("wal", "wals", newFileName)

	// Create and open the new file
	file, err := os.Create(fullPath)
	if err != nil {
		fmt.Println("Failed to create new WAL file:", err)
		return false
	}

	// Update WAL state
	wal.currentFile = file
	wal.walNames = append(wal.walNames, newFileName)

	return true
}

func (log *LogEntry) GetSerializedLogSize() int {
	keyBytes := []byte(log.Key)
	valueBytes := []byte(log.Value)

	return 8 + 1 + 8 + len(keyBytes) + 8 + len(valueBytes) + 4
}

func (log LogEntry) SerializeLogEntry() []byte {
	bytes := make([]byte, 0)

	// WHEN THE LOG WAS CREATED
	timestampBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestampBytes, uint64(log.Timestamp.Unix()))
	bytes = append(bytes, timestampBytes...)

	// TOMBSTONE, FOR NOW ONLY 0
	bytes = append(bytes, 0) // APPENDING FALSE FOR TOMBSTONE

	// KEY LENGHT AND LENGHT ITSELF
	keyBytes := []byte(log.Key)
	keyLen := len(keyBytes)
	keyLenBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(keyLenBytes, uint64(keyLen))
	bytes = append(bytes, keyLenBytes...)
	bytes = append(bytes, keyBytes...)

	// VALUE LENGHT AND VALUE ITSELF
	valueBytes := []byte(log.Value)
	valueLen := len(valueBytes)
	valueLenBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueLenBytes, uint64(valueLen))
	bytes = append(bytes, valueLenBytes...)
	bytes = append(bytes, valueBytes...)

	// CONTROL HASH SUM
	CRCbytes := make([]byte, 4)
	CRC := crc32.ChecksumIEEE(bytes)
	binary.LittleEndian.PutUint32(CRCbytes, uint32(CRC))
	bytes = append(CRCbytes, bytes...)

	return bytes
}

func DeserializeLogEntry(data []byte) (*LogEntry, error) {
	// CHECKING IF DATA HAS LENGHT OF MINIMAL LOG
	if len(data) < 37 {
		return nil, fmt.Errorf("data too short to be a valid log entry")
	}

	log := new(LogEntry)
	offset := 0

	// READING AND VALIDATING CRC
	expectedCRC := binary.LittleEndian.Uint32(data[0:4])
	computedCRC := crc32.ChecksumIEEE(data[4:])
	if expectedCRC != computedCRC {
		return nil, fmt.Errorf("CRC mismatch: expected %d, got %d", expectedCRC, computedCRC)
	}
	offset += 4

	// TIMESTAMP
	timestamp := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
	log.Timestamp = time.Unix(timestamp, 0)
	offset += 8

	// TOMBSTONE
	log.Tombstone = false
	if data[offset] != 0 {
		log.Tombstone = true
	}
	offset += 1

	// KEY
	keyLen := int(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8
	log.Key = string(data[offset : offset+keyLen])
	offset += keyLen

	// VALUE
	valueLen := int(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8
	log.Value = string(data[offset : offset+valueLen])

	return log, nil
}

func (wal WAL) PrinfOfFileNames() {
	for i := 0; i < len(wal.walNames); i++ {
		print(i + 1)
		print(". ")
		print(wal.walNames[i])
	}
}
