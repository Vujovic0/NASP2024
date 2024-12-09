package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"time"
)

type LogEntry struct {
	CRC       uint32
	Timestamp time.Time
	Tombstone bool
	Key       string
	Value     string
	Type      string
}

type WAL struct {
	walNames         []string
	blockSize        int
	segmentSize      int
	currentWalName   string
	currentBlock     int
	freeBlock        int
	minimumEntrySize int
}

func NewWAL(blockSize int, segmentSize int, currentBlock int, freeBlock int) *WAL {
	wal := new(WAL)
	/*file, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDONLY, 0666)
	if err != nil {
		fmt.Println(err)
		return nil
	}*/
	file, err1 := os.OpenFile("./wal/wals", os.O_RDONLY, 066)
	if err1 != nil {
		panic(err1)
	}
	namesOfWals, err2 := file.Readdirnames(0)
	if err2 != nil {
		panic(err2)
	}
	file.Close()
	wal.walNames = namesOfWals
	wal.segmentSize = segmentSize
	wal.blockSize = blockSize
	wal.currentBlock = currentBlock
	wal.freeBlock = freeBlock
	wal.currentWalName = namesOfWals[len(namesOfWals)-1]
	wal.minimumEntrySize = 35
	return wal
}

func NewLogEntry(key string, value string) *LogEntry {
	logEntry := new(LogEntry)
	logEntry.Key = key
	logEntry.Value = value
	logEntry.Tombstone = false
	logEntry.Timestamp = time.Now()
	logEntry.CRC = 0
	logEntry.Type = "FULL"
	return logEntry
}

func (log LogEntry) SerializeLogEntry() []byte {
	bytes := make([]byte, 0)
	CRCbytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(CRCbytes, uint32(log.CRC))
	bytes = append(bytes, CRCbytes...)

	typeBytes := []byte(log.Type)
	typeLen := len(typeBytes)
	typeLenBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(typeLenBytes, uint64(typeLen))
	bytes = append(bytes, typeLenBytes...)
	bytes = append(bytes, typeBytes...)

	timestampBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestampBytes, uint64(log.Timestamp.Unix()))
	bytes = append(bytes, timestampBytes...)

	bytes = append(bytes, 0) // APPENDING FALSE FOR TOMBSTONE

	keyBytes := []byte(log.Key)
	keyLen := len(keyBytes)
	keyLenBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(keyLenBytes, uint64(keyLen))
	bytes = append(bytes, keyLenBytes...)
	bytes = append(bytes, keyBytes...)

	valueBytes := []byte(log.Value)
	valueLen := len(valueBytes)
	valueLenBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueLenBytes, uint64(valueLen))
	bytes = append(bytes, valueLenBytes...)
	bytes = append(bytes, valueBytes...)

	return bytes
}

func (log *LogEntry) DeserializeLogEntry(file *os.File) error {
	CRCbytes := make([]byte, 4)
	_, err1 := file.Read(CRCbytes)
	if err1 != nil {
		panic(err1)
	}
	CRC := int32(binary.LittleEndian.Uint32(CRCbytes))

	typeLenBytes := make([]byte, 8)
	_, err01 := file.Read(typeLenBytes)
	if err01 != nil {
		panic(err01)
	}
	typeLen := int64(binary.LittleEndian.Uint64(typeLenBytes))

	typeBytes := make([]byte, typeLen)
	_, err02 := file.Read(typeBytes)
	if err02 != nil {
		panic(err02)
	}

	timeStampBytes := make([]byte, 8)
	_, err2 := file.Read(timeStampBytes)
	if err2 != nil {
		panic(err2)
	}
	timestamp := int64(binary.LittleEndian.Uint64(timeStampBytes))

	tombstoneBytes := make([]byte, 1)
	_, err3 := file.Read(tombstoneBytes)
	if err3 != nil {
		panic(err3)
	}
	var tombstone bool
	if tombstoneBytes[0] == 0 {
		tombstone = false
	} else {
		tombstone = true
	}

	keyLenBytes := make([]byte, 8)
	_, err6 := file.Read(keyLenBytes)
	if err6 != nil {
		panic(err6)
	}
	keyLen := int64(binary.LittleEndian.Uint64(keyLenBytes))

	keyBytes := make([]byte, keyLen)
	_, err7 := file.Read(keyBytes)
	if err7 != nil {
		panic(err7)
	}

	valueLenBytes := make([]byte, 8)
	_, err8 := file.Read(valueLenBytes)
	if err8 != nil {
		panic(err8)
	}
	valueLen := int64(binary.LittleEndian.Uint64(valueLenBytes))

	valueBytes := make([]byte, valueLen)
	_, err9 := file.Read(valueBytes)
	if err9 != nil {
		panic(err9)
	}

	key := string(keyBytes)
	value := string(valueBytes)
	typeB := string(typeBytes)

	log.CRC = uint32(CRC)
	log.Type = typeB
	log.Timestamp = time.Unix(timestamp, 0)
	log.Tombstone = tombstone
	log.Key = key
	log.Value = value
	return nil
}

func (wal *WAL) AddEntry(key string, value string) bool {
	logEntry := NewLogEntry(key, value)
	filename := "wal/wals/" + wal.currentWalName
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_RDONLY, 0666)
	if err != nil {
		fmt.Println(err)
		return false
	}
	bytes := logEntry.SerializeLogEntry()
	if len(bytes) <= wal.freeBlock && wal.currentBlock != wal.segmentSize {
		file.Write(bytes)
		fmt.Println(len(bytes))
		fmt.Println(wal.freeBlock)
		wal.freeBlock -= len(bytes)
		fmt.Println(wal.freeBlock)
	} else {
		padding := make([]byte, wal.freeBlock)
		file.Write(padding)
		wal.currentBlock += 1
		wal.freeBlock = wal.blockSize
		if wal.currentBlock == wal.segmentSize {
			file.Close()
			wal.currentBlock = 0
			newFileName := "wal" + strconv.Itoa(len(wal.walNames)+1) + ".log"
			wal.currentWalName = newFileName
			wal.walNames = append(wal.walNames, newFileName)
			_, err := os.Create("wal/wals/" + newFileName)
			if err != nil {
				panic(err)
			}
		} else {
			file.Write(bytes)
		}
	}
	if wal.freeBlock < wal.minimumEntrySize && wal.freeBlock != 0 {
		padding := make([]byte, wal.freeBlock)
		file.Write(padding)
		wal.currentBlock += 1
		wal.freeBlock = wal.blockSize
		if wal.currentBlock == wal.segmentSize {
			wal.currentBlock = 0
			file.Close()
			newFileName := "wal" + strconv.Itoa(len(wal.walNames)+1) + ".log"
			wal.currentWalName = newFileName
			wal.walNames = append(wal.walNames, newFileName)
			_, err := os.Create("wal/wals/" + newFileName)
			if err != nil {
				panic(err)
			}
		}
	}
	return true
}

func (wal WAL) PrinfOfFileNames() {
	for i := 0; i < len(wal.walNames); i++ {
		print(i + 1)
		print(". ")
		print(wal.walNames[i])
	}
}

func (wal WAL) ReadCurrentWALFile() { // ONLY A HELPER FUNCTION FOR DEBUGGING
	filename := "wal/wals/" + wal.currentWalName
	file, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	log1 := LogEntry{}
	log2 := LogEntry{}

	file.Seek(0, 0)

	log1.DeserializeLogEntry(file)
	log2.DeserializeLogEntry(file)
	fmt.Println(log1.Key)
	fmt.Println(log1.Value)
	fmt.Println(log1.Timestamp)
	fmt.Println(log2.Key)
	fmt.Println(log2.Value)
	fmt.Println(log2.Timestamp)
	file.Close()

}
