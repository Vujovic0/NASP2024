package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
)

const (
	SEGMENT_SIZE     = 20 // The size of segment in bytes
	BLOCK_SIZE       = 50
	PAGE_SIZE        = 20
	BUFFER_SIZE      = FILE_SIZE*PAGE_SIZE + 1 // The size of one block is enough for putting all headers
	FILE_SIZE        = 10
	FACTOR_MOVEMENT  = SEGMENT_SIZE*BLOCK_SIZE*PAGE_SIZE + 2*(1+PAGE_SIZE) // NUMBER OF BYTES IN PAGE PLUS NUMBER OF BYTES FOR HEADERS OF PAGES AND BLOCKS, EVERY PROCESS OF MAKING NEW PAGE REQUIRES POINTERS TO MOVE BY THIS VALUE
	BLOCK_SIZE_BYTES = SEGMENT_SIZE*BLOCK_SIZE + 2                         // Size of block in bytes plus header
)

type BufferPool struct {
	flush   int
	start   int
	end     int
	blockId int
	pageId  int
	fileID  int
	buffer  []byte
}

func newBufferPool() *BufferPool {
	return &BufferPool{
		start:   2,
		flush:   2, // Making space for page header
		end:     FACTOR_MOVEMENT,
		blockId: 0,
		pageId:  0,
		fileID:  0,
		buffer:  make([]byte, BUFFER_SIZE*BLOCK_SIZE*SEGMENT_SIZE),
	}
}

func ceil(a, b int) int {
	return (a + b - 1) / b
}

func flush(bufferPool *BufferPool) error {
	filename := "data_file_" + strconv.Itoa(bufferPool.fileID) + ".bin"
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()
	// Write the fileID as a 4-byte integer at the start
	if err := binary.Write(file, binary.LittleEndian, int32(bufferPool.fileID)); err != nil {
		return fmt.Errorf("failed to write fileID to file: %v", err)
	}
	// Write the buffer contents up to the current 'end' pointer to the file
	if _, err := file.Write(bufferPool.buffer[:bufferPool.end]); err != nil {
		return fmt.Errorf("failed to write buffer to file: %v", err)
	}
	// Reset the buffer and state after flushing
	bufferPool.start = 2
	bufferPool.end = FACTOR_MOVEMENT
	bufferPool.flush = 2
	bufferPool.blockId = 0
	bufferPool.pageId = 0
	bufferPool.fileID++ // Increment fileID for the next flush

	return nil
}

func addBlockCheck(bufferPool *BufferPool, logSize int, status int, data []byte) {
	headerBlock := make([]byte, 2)
	headerBlock = append(headerBlock, byte(status), byte(logSize))
	copy(bufferPool.buffer[bufferPool.start:], headerBlock)
	bufferPool.start += 2
	copy(bufferPool.buffer[bufferPool.start:], data)
	bufferPool.start += logSize * SEGMENT_SIZE
	if logSize < BLOCK_SIZE {
		filler := make([]byte, BLOCK_SIZE-logSize)
		copy(bufferPool.buffer[bufferPool.start:], filler)
		bufferPool.start += (BLOCK_SIZE - logSize) * SEGMENT_SIZE
	}
	bufferPool.blockId += 1
}

func addBlock(bufferPool *BufferPool, status int, data []byte) {
	headerBlock := make([]byte, 2)
	headerBlock = append(headerBlock, byte(status), byte(BLOCK_SIZE))
	copy(bufferPool.buffer[bufferPool.start:], headerBlock)
	bufferPool.start += 2
	copy(bufferPool.buffer[bufferPool.start:], data)
	bufferPool.start += BLOCK_SIZE * SEGMENT_SIZE
	bufferPool.blockId += 1
}

func inputLog(bufferPool *BufferPool, data []byte) error {
	logSize := len(data) / SEGMENT_SIZE
	available := PAGE_SIZE - bufferPool.blockId
	needed := ceil(logSize, BLOCK_SIZE)
	if available < needed {
		headerPage := make([]byte, 2)
		headerPage = append(headerPage, byte(bufferPool.pageId), byte(PAGE_SIZE-ceil(logSize, BLOCK_SIZE)))
		position := bufferPool.pageId * FACTOR_MOVEMENT
		copy(bufferPool.buffer[position:], headerPage)
		fillerPage := make([]byte, BLOCK_SIZE_BYTES*available)
		copy(bufferPool.buffer[bufferPool.start:], fillerPage)
		bufferPool.blockId = 0
		bufferPool.pageId += 1
		bufferPool.end += FACTOR_MOVEMENT
		bufferPool.flush += FACTOR_MOVEMENT
		bufferPool.start += BLOCK_SIZE_BYTES * available
		if bufferPool.pageId >= FILE_SIZE {
			flush(bufferPool)
		}
	}
	if logSize == 0 {
		return fmt.Errorf("log data is empty; cannot create a block")
	} else if logSize <= BLOCK_SIZE {
		addBlockCheck(bufferPool, logSize, 0, data)
	} else if logSize <= 2*BLOCK_SIZE {
		blockLimit := BLOCK_SIZE_BYTES
		addBlock(bufferPool, 1, data[:blockLimit])
		addBlockCheck(bufferPool, logSize, 3, data[blockLimit:])
	} else if logSize <= 3*BLOCK_SIZE {
		blockLimit1 := BLOCK_SIZE_BYTES
		blockLimit2 := 2 * BLOCK_SIZE_BYTES
		addBlock(bufferPool, 1, data[:blockLimit1])
		addBlock(bufferPool, 2, data[blockLimit1:blockLimit2])
		addBlockCheck(bufferPool, logSize, 3, data[blockLimit2:])
	} else {
		return fmt.Errorf("log data exceeds the maximum size of 3 blocks")
	}
	if bufferPool.start == bufferPool.end {
		headerPage := make([]byte, 2)
		headerPage = append(headerPage, byte(bufferPool.pageId), byte(PAGE_SIZE))
		position := bufferPool.pageId * FACTOR_MOVEMENT
		copy(bufferPool.buffer[position:], headerPage)
		bufferPool.blockId = 0
		bufferPool.pageId += 1
		bufferPool.end += FACTOR_MOVEMENT
		bufferPool.flush += FACTOR_MOVEMENT
		if bufferPool.pageId >= FILE_SIZE {
			flush(bufferPool)
		}
	}
	return nil
}

func readPageFromBufferPool(bufferPool *BufferPool, pageId int) ([]byte, error) {
	pageSize := PAGE_SIZE * BLOCK_SIZE
	offset := pageId * pageSize
	if offset >= bufferPool.start && offset < bufferPool.end {
		pageData := bufferPool.buffer[offset : offset+pageSize]
		return pageData, nil
	} else {
		return nil, fmt.Errorf("page %d not found in buffer pool", pageId)
	}
}

func readPageFromDisk(pageId int, filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	pageSize := PAGE_SIZE * BLOCK_SIZE
	offset := pageId * pageSize
	pageData := make([]byte, pageSize)
	_, err = file.ReadAt(pageData, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("failed to read page from file: %v", err)
	}

	return pageData, nil
}
