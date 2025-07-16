package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Vujovic0/NASP2024/blockManager"
	"github.com/Vujovic0/NASP2024/memtableStructures"
)

const BlockHeaderSize = 9

type LogEntry struct {
	Timestamp time.Time
	Tombstone bool
	Key       string
	Value     []byte
}

func (le LogEntry) String() string {
	return fmt.Sprintf("LogEntry(Key: %s, Value: %d)", le.Key, string(le.Value))
}

type WAL struct {
	walNames         []string // NAMES OF ALL WALx.LOG
	blockSize        int      //
	segmentSize      int      // HOW MANY BLOCKS WE HAVE IN ONE FILE
	CurrentFile      *os.File // POINTER TO A FILE THAT WE ARE CURRENTLY USING
	CurrentBlock     int      // OFFSET WHERE WE WRITE
	minimumEntrySize int      // HELPER ATTRIBUTE FOR PADDING
}

// OFFSET.BIN BLOCK DATA CONTAINS:
// 1. NAME OF THE FIRST WAL FILE THAT WE NEED TO LOAD
// 2. BLOCK INDEX THAT WE NEED TO LOAD FROM
// 3. LOG OFFSET FROM THAT BLOCK
// 4B NAME LENGHT | ?B NAME ITSELF | 4B BLOCK INDEX | 8B LOG OFFSET
// IF OFFSET.BIN IS NOT FOUND, WE CREATE IT AND INITIALIZE THE FIRST WAL1.LOG

func NewWAL(blockSize int, segmentSize int) *WAL {
	wal := new(WAL)

	//fileIndex, blockNumber, logOffset := LoadOffset()

	// DODAJ DA SE PROCITANI OFFSET UBACUJE U WAL
	//wal.walNames = namesOfWals
	wal.segmentSize = segmentSize
	wal.blockSize = blockSize
	wal.CurrentBlock = 0
	//wal.currentWalName = namesOfWals[len(namesOfWals)-1]
	// DODAJ DA JE CURRENTWAL POKAZIVAČ NA OS.FILE
	wal.minimumEntrySize = 35
	offsetFilePath := "./wal/offset.bin"

	file, err := os.OpenFile(offsetFilePath, os.O_RDONLY, 066)
	if os.IsNotExist(err) {
		wal.createFirstOffsetFile()
	} else if err != nil {
		panic(err)
	} else {
		defer file.Close()
		// WE WILL DESERIALIZE OFFSET FILE LATER IN WAL LOADING
	}

	wal.walNames, wal.CurrentFile, _ = getWALFiles("./wal/wals/")
	fmt.Print(wal.walNames)
	return wal
}

func getWALFiles(folderPath string) ([]string, *os.File, error) {
	entries, err := os.ReadDir(folderPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read folder: %w", err)
	}

	var fileNames []string

	for _, entry := range entries {
		if !entry.IsDir() {
			fileNames = append(fileNames, entry.Name())
		}
	}

	sort.Strings(fileNames)

	if len(fileNames) == 0 {
		return nil, nil, fmt.Errorf("no files found in %s", folderPath)
	}

	lastFileName := fileNames[len(fileNames)-1]
	lastFilePath := folderPath + "/" + lastFileName

	file, err := os.OpenFile(lastFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return fileNames, nil, fmt.Errorf("failed to open last WAL file: %w", err)
	}

	return fileNames, file, nil
}

func (wal *WAL) createFirstOffsetFile() {
	nameBytes := []byte("wal1.log")
	nameLen := uint32(len(nameBytes))

	result := make([]byte, wal.blockSize)

	binary.LittleEndian.PutUint32(result[0:4], nameLen)
	copy(result[4:4+len(nameBytes)], nameBytes)
	binary.LittleEndian.PutUint32(result[4+len(nameBytes):8+len(nameBytes)], 0)
	binary.LittleEndian.PutUint64(result[8+len(nameBytes):], 0)

	blockType := make([]byte, 1)
	blockType[0] = 0

	blockData := wal.BuildBlock(blockType, result)

	block := blockManager.InitBlock("offset.bin", 0, blockData)

	file, err := os.Create("wal/offset.bin")
	if err != nil {
		panic("Failed to open offset.bin: " + err.Error())
	}
	defer file.Close()

	blockManager.WriteBlock(file, block)

	wal.CurrentFile, err = os.Create("wal/wals/wal1.log")
	if err != nil {
		panic("Failed to create wal1.log: " + err.Error())
	}
}

func (wal *WAL) DeserializeOffsetFile() (string, uint32, uint64) {
	file, err := os.Open("wal/offset.bin")
	if err != nil {
		panic("Failed to open offset.bin: " + err.Error())
	}
	defer file.Close()

	block := blockManager.ReadBlock(file, 0)

	blockData := block.GetData()
	// FIRST 9 BYTES ARE HEADER
	nameLen := binary.LittleEndian.Uint32(blockData[9:13])

	name := string(blockData[13 : 13+nameLen])
	blockIdx := binary.LittleEndian.Uint32(blockData[13+nameLen : 17+nameLen])
	logOffset := binary.LittleEndian.Uint64(blockData[17+nameLen:])
	fmt.Println("Učitavanje offseta")
	return name, blockIdx, logOffset

}

func NewLogEntry(key string, value []byte, tombstone bool) *LogEntry {
	logEntry := new(LogEntry)
	logEntry.Key = key
	logEntry.Value = value
	logEntry.Tombstone = false
	logEntry.Timestamp = time.Now()
	return logEntry
}

func (wal *WAL) WriteLogEntry(key string, value []byte, tombstone bool) (int, error) {
	log := NewLogEntry(key, value, tombstone)
	logEntryBytes := log.SerializeLogEntry()
	logSizeNeeded := log.GetSerializedLogSize()
	var offset int
	var err error
	if logSizeNeeded < wal.blockSize-9 {
		offset, err = wal.WriteLogEntryType0(logEntryBytes, logSizeNeeded)
	} else {
		offset, err = wal.WriteLongEntryOtherType(logEntryBytes, logSizeNeeded)
	}
	return offset, err
}

func (wal *WAL) WriteLongEntryOtherType(logEntryBytes []byte, logSizeNeeded int) (int, error) {
	currentBlockData := blockManager.ReadBlock(wal.CurrentFile, uint64(wal.CurrentBlock))

	if currentBlockData != nil {
		wal.CurrentBlock += 1
	}
	if wal.CurrentBlock == wal.segmentSize {
		wal.InitNewFile()
	}

	// HELPER INTS TO TRACK WHICH PARTS OF LOG HAVE WE WRITTEN
	startOffset := 0
	stopOffset := wal.blockSize - BlockHeaderSize

	blockType := make([]byte, 1)
	blockType[0] = 1
	// WRITING THE FIRST TYPE 1 BLOCK
	blockData := wal.BuildBlock(blockType, logEntryBytes[startOffset:stopOffset])
	block := blockManager.InitBlock(wal.CurrentFile.Name(), uint64(wal.CurrentBlock), blockData)
	blockManager.WriteBlock(wal.CurrentFile, block)
	wal.CurrentFile.Sync()
	// WRITING TYPE 2 BLOCKS UNTIL THE LAST PART OF LOG CAN FIT INTO A SINGLE BLOCK
	blockType[0] = 2
	for {
		startOffset = stopOffset
		stopOffset += wal.blockSize - BlockHeaderSize
		if (logSizeNeeded - startOffset) <= (wal.blockSize - BlockHeaderSize) {
			break
		}
		wal.CurrentBlock += 1
		if wal.CurrentBlock == wal.segmentSize {
			wal.InitNewFile()
		}
		blockData := wal.BuildBlock(blockType, logEntryBytes[startOffset:stopOffset])
		block := blockManager.InitBlock(wal.CurrentFile.Name(), uint64(wal.CurrentBlock), blockData)
		blockManager.WriteBlock(wal.CurrentFile, block)
		wal.CurrentFile.Sync()
	}
	// WHEN WE FINISH WITH ALL TYPE 2 BLOCKS, WE WRITE FINAL TYPE 3 BLOCK
	wal.CurrentBlock += 1
	if wal.CurrentBlock == wal.segmentSize {
		wal.InitNewFile()
	}
	blockType[0] = 3
	blockData = wal.BuildBlock(blockType, logEntryBytes[startOffset:])
	block = blockManager.InitBlock(wal.CurrentFile.Name(), uint64(wal.CurrentBlock), blockData)
	blockManager.WriteBlock(wal.CurrentFile, block)
	wal.CurrentFile.Sync()
	return 0, nil
}

func (wal *WAL) WriteLogEntryType0(logEntryBytes []byte, logSizeNeeded int) (int, error) {
	currentBlock := blockManager.ReadBlock(wal.CurrentFile, uint64(wal.CurrentBlock))

	// IF WE ARE AT THE START OF AN EMPTY FILE
	if currentBlock == nil {
		blockType := make([]byte, 1)
		blockType[0] = 0
		blockData := wal.BuildBlock(blockType, logEntryBytes)
		block := blockManager.InitBlock(wal.CurrentFile.Name(), uint64(wal.CurrentBlock), blockData)
		blockManager.WriteBlock(wal.CurrentFile, block)
		wal.CurrentFile.Sync()
		return 9 + block.GetSize(), nil
	}

	if currentBlock.GetType() == 0 { // IF CURRENT BLOCK IS FILLED WITH THE END OF THE OLDER LOG
		// PROMENI PROVERU TAKO ŠTO GLEDAŠ KOLIKO JE ZAPISANO U BLOCKU
		if wal.blockSize-currentBlock.GetSize()-9 >= logSizeNeeded {
			// WRITING IN THE SAME BLOCK
			blockType := make([]byte, 1)
			blockType[0] = 0
			// DODAJ DA SE VELIČINA PODATAKA PROSLEĐUJE U BUILD BLOCK I RUČNO AŽURIRA
			newBlockBytes := wal.UpdateBlock(blockType, currentBlock.GetData(), currentBlock.GetSize(), logEntryBytes)
			block := blockManager.InitBlock(wal.CurrentFile.Name(), uint64(wal.CurrentBlock), newBlockBytes)
			blockManager.WriteBlock(wal.CurrentFile, block)
			wal.CurrentFile.Sync()
			return 9 + block.GetSize(), nil
		} else {
			wal.CurrentBlock += 1
			if wal.CurrentBlock == wal.segmentSize {
				// WRITING IN NEW WAL FILE
				wal.InitNewFile()
				blockType := make([]byte, 1)
				blockType[0] = 0
				newBlockBytes := wal.BuildBlock(blockType, logEntryBytes)
				block := blockManager.InitBlock(wal.CurrentFile.Name(), uint64(wal.CurrentBlock), newBlockBytes)
				blockManager.WriteBlock(wal.CurrentFile, block)
				wal.CurrentFile.Sync()
				return 9 + block.GetSize(), nil
			} else {
				// WRITING IN NEW BLOCK IN SAME WAL FILE
				blockType := make([]byte, 1)
				blockType[0] = 0
				blockData := wal.BuildBlock(blockType, logEntryBytes)
				block := blockManager.InitBlock(wal.CurrentFile.Name(), uint64(wal.CurrentBlock), blockData)
				blockManager.WriteBlock(wal.CurrentFile, block)
				wal.CurrentFile.Sync()
				return 9 + block.GetSize(), nil
			}
		}
	} else {
		// CASE WHERE WE JUST WRITE IN A NEW BLOCK
		wal.CurrentBlock += 1
		if wal.CurrentBlock == wal.segmentSize {
			wal.InitNewFile()
		}
		blockType := make([]byte, 1)
		blockType[0] = 0
		newBlockBytes := wal.BuildBlock(blockType, logEntryBytes)
		block := blockManager.InitBlock(wal.CurrentFile.Name(), uint64(wal.CurrentBlock), newBlockBytes)
		blockManager.WriteBlock(wal.CurrentFile, block)
		wal.CurrentFile.Sync()
		return 9 + block.GetSize(), nil
	}
}

func (wal *WAL) BuildBlock(blockType []byte, blockData []byte) []byte {
	dataSize := uint32(len(blockData))
	block := make([]byte, wal.blockSize)
	block[4] = blockType[0]
	binary.LittleEndian.PutUint32(block[5:BlockHeaderSize], dataSize)
	copy(block[9:], blockData)
	crc := crc32.ChecksumIEEE(block[4:])
	binary.LittleEndian.PutUint32(block[0:4], crc)
	return block
}

func (wal *WAL) UpdateBlock(blockType []byte, blockData []byte, oldSize int, newData []byte) []byte {
	dataSize := oldSize + len(newData)
	block := make([]byte, wal.blockSize)
	block[4] = blockType[0]
	binary.LittleEndian.PutUint32(block[5:BlockHeaderSize], uint32(dataSize))

	copy(block[9:], blockData[9:9+oldSize]) // OLD DATA
	copy(block[9+oldSize:], newData)        // NEW DATA

	crc := crc32.ChecksumIEEE(block[4:])
	binary.LittleEndian.PutUint32(block[0:4], crc)
	return block
}

func (wal *WAL) InitNewFile() bool {
	wal.CurrentBlock = 0 // RESETTING BLOCK INDEX FOR THE NEW FILE
	if wal.CurrentFile != nil {
		wal.CurrentFile.Close()
	}

	// GET LAST WAL NUMBER
	base := strings.TrimSuffix(wal.walNames[len(wal.walNames)-1], ".log") // CUTTING OFF .BIN
	numberStr := strings.TrimPrefix(base, "wal")                          // CUTTING OFF "wal"
	number, err := strconv.Atoi(numberStr)                                // CONVERTING TO INT
	if err != nil {
		fmt.Println("Failed to parse current WAL number:", err)
		return false
	}

	newFileName := fmt.Sprintf("wal%d.log", number+1)
	fullPath := "./wal/wals/" + newFileName

	// CREATING NEW WAL
	file, err := os.Create(fullPath)
	if err != nil {
		fmt.Println("Failed to create new WAL file:", err)
		return false
	}

	// UPDATE WAL
	wal.CurrentFile = file
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
	//valueBytes := []byte(log.Value)
	valueLen := len(log.Value)
	valueLenBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueLenBytes, uint64(valueLen))
	bytes = append(bytes, valueLenBytes...)
	bytes = append(bytes, log.Value...)

	// CONTROL HASH SUM
	CRCbytes := make([]byte, 4)
	CRC := crc32.ChecksumIEEE(bytes)
	binary.LittleEndian.PutUint32(CRCbytes, uint32(CRC))
	bytes = append(CRCbytes, bytes...)

	DeserializeLogEntry(bytes)

	return bytes
}

func DeserializeLogEntry(data []byte) (LogEntry, bool) {
	if len(data) < 4+8+1+8+8 {
		return LogEntry{}, false
	}

	// CRC CHECK FIRST
	expectedCRC := binary.LittleEndian.Uint32(data[:4])
	payload := data[4:]
	actualCRC := crc32.ChecksumIEEE(payload)
	if expectedCRC != actualCRC {
		//return LogEntry{}, false
	}

	offset := 0

	// TIMESTAMP
	timestampUnix := int64(binary.LittleEndian.Uint64(payload[offset : offset+8]))
	if timestampUnix == 0 {
		return LogEntry{}, false
	}
	timestamp := time.Unix(timestampUnix, 0)
	offset += 8

	// TOMBSTONE
	tombstone := payload[offset] != 0
	offset += 1

	// KEY LENGTH
	if offset+8 > len(payload) {
		return LogEntry{}, false
	}
	keyLen := int(binary.LittleEndian.Uint64(payload[offset : offset+8]))
	offset += 8
	if offset+keyLen > len(payload) {
		return LogEntry{}, false
	}
	key := string(payload[offset : offset+keyLen])
	offset += keyLen

	// VALUE LENGTH
	if offset+8 > len(payload) {
		return LogEntry{}, false
	}
	valueLen := int(binary.LittleEndian.Uint64(payload[offset : offset+8]))
	offset += 8
	if offset+valueLen > len(payload) {
		return LogEntry{}, false
	}
	value := payload[offset : offset+valueLen]

	return LogEntry{
		Timestamp: timestamp,
		Tombstone: tombstone,
		Key:       key,
		Value:     value,
	}, true
}

func CalculateSerializedLogSize(key string, value []byte) int {
	keyBytes := []byte(key)
	//valueBytes := []byte(value)

	totalSize := 4 + 8 + 1 + 8 + len(keyBytes) + 8 + len(value)
	return totalSize
}

func deleteFile(path string) error {
	err := os.Remove(path)
	if err != nil {
		fmt.Println("Failed to delete file %s: %v", path, err)
		return err
	}
	// fmt.Println("obrisan wal fajl")
	return nil
}

func (wal *WAL) deleteOldWALSegments(fileIndex int) {
	// WE DELETE FROM THE GIVEN FILE INDEX
	for i := 0; i < fileIndex; i++ {
		pathName := "./wal/wals/" + wal.walNames[i]
		err := deleteFile(pathName)
		if err != nil {
			fmt.Println("Continuing old WAL segments deleting...")
		}
	}
}

func (wal *WAL) LoadWALLogs(memtable *memtableStructures.MemTableManager) {
	// ARRAY THAT'S GOING TO BE USED TO STORE DATA FROM MULTIPLE BLOCKS OR EVEN FILES
	// DODAJ BRISANJE STARIH WALOVA
	var wholeBlockData []byte
	var reading bool
	loadingSegmentNameFull, loadingBlock, loadingOffset := wal.DeserializeOffsetFile()
	var startingFileIndex int
	loadingSegmentNameSplited := strings.Split(loadingSegmentNameFull, "/")
	loadingSegmentName := loadingSegmentNameSplited[len(loadingSegmentNameSplited)-1]
	for fileIndex := 0; fileIndex < len(wal.walNames); fileIndex++ {
		if wal.walNames[fileIndex] == loadingSegmentName {
			startingFileIndex = fileIndex
			break
		}
	}
	for fileIndex := startingFileIndex; fileIndex < len(wal.walNames); fileIndex++ {
		fullPath := "./wal/wals/" + wal.walNames[fileIndex]
		file, err := os.Open(fullPath)
		if err != nil {
			fmt.Errorf("failed to open WAL file %s: %v", wal.walNames[fileIndex], err)
			fmt.Println(err)
		}

		if fileIndex == startingFileIndex {
			wal.CurrentBlock = int(loadingBlock)
			// IF THE LAST BLOCK IS TYPE 3, THAT MEANS THAT WE JUST SKIP IT AND LOAD NEXT ONE
			// IF IT'S TYPE 0, THERE IS MAYBE MORE DATA TO BE EXTRACTED FROM THAT BLOCK
			lastUsedBlock := blockManager.ReadBlock(file, uint64(wal.CurrentBlock))
			if lastUsedBlock == nil {
				// CASE IF WE TRY TO THE FILE THAT IS EMPTY AND FIRST
				wal.deleteOldWALSegments(startingFileIndex)
				return
			}
			if lastUsedBlock.GetType() != 0 {
				wal.CurrentBlock += 1
				if wal.CurrentBlock == wal.segmentSize {
					wal.CurrentBlock = 0
					wal.deleteOldWALSegments(startingFileIndex)
					continue
				}
			}
		} else {
			wal.CurrentBlock = 0
		}

		for {
			block := blockManager.ReadBlock(file, uint64(wal.CurrentBlock))
			if block == nil {
				if wal.CurrentBlock == 0 {
					// IF IT'S 0, IT JUST MEANS IT'S EMPTY FILE, WE DON'T DO ANYTHING ABOUT IT
					wal.deleteOldWALSegments(startingFileIndex)
					return
				} else {
					// WE JUST GO BACK TO THE LAST BLOCK THAT HAS SOMETHING IN IT
					wal.CurrentBlock -= 1
					wal.deleteOldWALSegments(startingFileIndex)
					return
				}
			}
			switch block.GetType() {
			case 0:
				var offset int
				if fileIndex == startingFileIndex {
					offset = int(loadingOffset)
				} else {
					offset = 9 // SO WE CAN SKIP HEADER
				}

				for {
					log, isRead := DeserializeLogEntry(block.GetData()[offset:])
					if isRead {
						fmt.Println(log)
						logSize := CalculateSerializedLogSize(log.Key, log.Value)
						offset += logSize
						memtable.Insert(log.Key, log.Value, log.Tombstone, wal.walNames[fileIndex], wal.CurrentBlock, offset)
					} else {
						break
					}
				}
				wal.CurrentBlock += 1
			case 1:
				if reading {
					// STAVI GREŠKU ZA TO DA JE VEĆ U PROCESU ČITANJE VIŠEBLOKOVSKOG LOGA,
					// A OPET SMO NAIŠLI NA BLOCK  TYPE 1
				}
				reading = true
				wholeBlockData = make([]byte, 0)
				wholeBlockData = append(wholeBlockData, block.GetData()[9:]...)
				wal.CurrentBlock += 1
			case 2:
				if !reading {
					// STAVI GREŠKU ZA TO DA JE VEĆ U PROCESU ČITANJE VIŠEBLOKOVSKOG LOGA,
					// A NISMO NAIŠLI NA BLOCK  TYPE 2
				}
				wholeBlockData = append(wholeBlockData, block.GetData()[9:]...)
				wal.CurrentBlock += 1
			case 3:
				if !reading {
					// STAVI GREŠKU ZA TO DA JE VEĆ U PROCESU ČITANJE VIŠEBLOKOVSKOG LOGA,
					// A NISMO NAIŠLI NA BLOCK  TYPE 3
				}
				wholeBlockData = append(wholeBlockData, block.GetData()[9:]...)
				reading = false
				log, _ := DeserializeLogEntry(wholeBlockData)
				fmt.Println(log)
				memtable.Insert(log.Key, log.Value, log.Tombstone, wal.walNames[fileIndex], wal.CurrentBlock, 0)
				wal.CurrentBlock += 1
			}
			if wal.CurrentBlock == wal.segmentSize {
				break
			}
		}

	}
	if wal.CurrentBlock == 0 {
		// IF IT'S 0, IT JUST MEANS IT'S EMPTY FILE, WE DON'T DO ANYTHING ABOUT IT
		wal.deleteOldWALSegments(startingFileIndex)
		return
	} else {
		// WE JUST GO BACK TO THE LAST BLOCK THAT HAS SOMETHING IN IT
		wal.CurrentBlock -= 1
		wal.deleteOldWALSegments(startingFileIndex)
		return
	}
}
