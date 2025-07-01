package utils

import (
	"NASP2024/memtableStructures"
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"strconv"
	"strings"
)

type Entry = memtableStructures.Element

func ParseElementFromString(s string) memtableStructures.Element {
	parts := strings.Split(s, "|")
	if len(parts) != 4 {
		panic("Invalid element string format: expected 4 parts, got " + strconv.Itoa(len(parts)))
	}

	timestamp, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		panic("Invalid timestamp in element: " + err.Error())
	}

	tombstone := parts[3] == "true"

	return memtableStructures.Element{
		Key:       parts[0],
		Value:     parts[1],
		Timestamp: timestamp,
		Tombstone: tombstone,
	}
}

func SerializeEntry(e *Entry, includeHeader bool) []byte {
	var buf bytes.Buffer
	key := []byte(e.Key)
	value := []byte(e.Value)

	if includeHeader {
		// Header: CRC | TIMESTAMP | TOMBSTONE | KEYSIZE | VALUESIZE
		header := make([]byte, 29)
		binary.LittleEndian.PutUint64(header[4:], uint64(e.Timestamp))
		if e.Tombstone {
			header[12] = 1
		} else {
			header[12] = 0
		}
		binary.LittleEndian.PutUint64(header[13:], uint64(len(key)))
		binary.LittleEndian.PutUint64(header[21:], uint64(len(value)))
		buf.Write(header)
	}
	buf.Write(key)
	buf.Write(value)

	if includeHeader {
		data := buf.Bytes()
		checksum := crc32.ChecksumIEEE(data[4:])
		binary.LittleEndian.PutUint32(data[:4], checksum)
		return data
	}
	return buf.Bytes()
}
