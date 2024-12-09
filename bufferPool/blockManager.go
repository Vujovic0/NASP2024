package main

import "os"

type BlockManager struct {
	file *os.File
}

func NewBlockManager(filename string) (*BlockManager, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &BlockManager{file: file}, nil
}

func (bm *BlockManager) WriteBlock(offset int64, data []byte) error {
	_, err := bm.file.WriteAt(data, offset)
	return err
}

func (bm *BlockManager) ReadBlock(offset int64, size int) ([]byte, error) {
	data := make([]byte, size)
	_, err := bm.file.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}
	return data, nil
}
