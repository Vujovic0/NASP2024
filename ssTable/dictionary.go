package ssTable

import (
	"encoding/gob"
	"os"
	"sync"
)

var globalDict *Dictionary

func SetGlobalDict(dict *Dictionary) {
	globalDict = dict
}

func GetGlobalDict() *Dictionary {
	if globalDict == nil {
		panic("globalDict is nil — call SetGlobalDict first")
	}
	return globalDict
}

type Dictionary struct {
	KeyToID    map[string]int `json:"key_to_id"`
	IDToKey    map[int]string `json:"id_to_key"`
	NextID     int            `json:"next_id"`
	mutex      sync.RWMutex
	ForwardMap map[string]int
	ReverseMap map[int]string
}

func NewDictionary() *Dictionary {
	return &Dictionary{
		KeyToID: make(map[string]int),
		IDToKey: make(map[int]string),
		NextID:  1,
	}
}

func (d *Dictionary) GetOrAdd(key string) int {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if id, exists := d.KeyToID[key]; exists {
		return id
	}

	id := d.NextID
	d.NextID++
	d.KeyToID[key] = id
	d.IDToKey[id] = key
	return id
}

func (d *Dictionary) Resolve(id int) (string, bool) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	key, exists := d.IDToKey[id]
	return key, exists
}

func (d *Dictionary) LoadFromDisk(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	dec := gob.NewDecoder(file)
	return dec.Decode(d)
}

func (d *Dictionary) SaveToDisk(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	enc := gob.NewEncoder(file)
	return enc.Encode(d)
}
