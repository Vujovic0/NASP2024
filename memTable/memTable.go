package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"time"
)

type Config struct {
	WalSize           uint64 `json:"wal_size"`
	MemtableSize      uint64 `json:"memtable_size"`
	MemtableStructure string `json:"memtable_structure"`
}

type MemoryTable struct {
	Data        interface{} // Moze biti SkipList, BTree ili HashMap
	MaxSize     uint64
	Structure   string // Moze biti "btree","skiplist" ili "hashmap"
	CurrentSize int
}

type Element struct {
	Key       string
	Value     string
	Timestamp int64
	Tombstone bool
}

func initializeMemoryTable() *MemoryTable {
	var config Config
	configData, err := os.ReadFile("config (1).json")
	if err != nil {
		log.Fatal(err)
	}
	err = json.Unmarshal(configData, &config)
	if err != nil {
		log.Fatal("Error parsing config: ", err)
	}
	fmt.Println(config)

	var memTable *MemoryTable

	switch config.MemtableStructure {
	case "btree":
		memTable = initializeBTreeMemTable(&config)
	case "skiplist":
		memTable = initializeSkipListMemTable(&config)
	case "hashmap":
		memTable = initializeHashMapMemtable(&config)
	default:
		log.Fatal("Nepoznata struktura memtable: ", config.MemtableStructure)
	}

	return memTable
}

// Metode koje moraju imati skiplist i btree
type Memtable interface {
	Insert(key string, value string, timestamp int64, tombstone bool)
	Search(key string) (*Element, bool)
	Delete(key string)
}

func initializeSkipListMemTable(config *Config) *MemoryTable {
	// Kreiranje SkipList-e
	skipList := newSkipList(16) // Primer maksimalne visine
	memTable := &MemoryTable{
		Data:        skipList,
		MaxSize:     config.MemtableSize,
		Structure:   config.MemtableStructure,
		CurrentSize: 0,
	}
	fmt.Println("Memtable initialized with config: ", config)

	return memTable
}

func initializeBTreeMemTable(config *Config) *MemoryTable {
	// Kreiranje BTree-a
	BTree := newBTree(16)
	memTable := &MemoryTable{
		Data:        BTree,
		MaxSize:     config.MemtableSize,
		Structure:   config.MemtableStructure,
		CurrentSize: 0,
	}
	fmt.Println("Memtable initialized with config: ", config)

	return memTable
}

func initializeHashMapMemtable(config *Config) *MemoryTable {
	// Kreiranje HashMape
	hashMap := newHashMap(16)
	memTable := &MemoryTable{
		Data:        hashMap,
		MaxSize:     config.MemtableSize,
		Structure:   config.MemtableStructure,
		CurrentSize: 0,
	}
	fmt.Println("Memtable initialized with config: ", config)

	return memTable
}

func (mt *MemoryTable) Insert(key string, value string) {
	element := Element{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().Unix(),
		Tombstone: false,
	}

	if mt.Structure == "skiplist" {
		skipList := mt.Data.(*SkipList)
		skipList.insert(element.Key, element.Value, element.Timestamp, element.Tombstone)
		mt.CurrentSize += 1
		if mt.CurrentSize >= int(mt.MaxSize) {
			mt.Flush()
		}
	} else if mt.Structure == "btree" {
		bTree := mt.Data.(*BTree)
		bTree.insert(element)

		mt.CurrentSize += 1
		if mt.CurrentSize >= int(mt.MaxSize) {
			mt.Flush()
		}
	} else if mt.Structure == "hashmap" {
		hashMap := mt.Data.(*HashMap)
		hashMap.insert(element)

		mt.CurrentSize += 1
		if mt.CurrentSize >= int(mt.MaxSize) {
			mt.Flush()
		}

	} else {
		log.Fatal("Nepoznata struktura memtable")
	}
}

func (mt *MemoryTable) Search(key string) (*Element, bool) {

	if mt.Structure == "skiplist" {
		sl := mt.Data.(*SkipList)
		value, found := sl.search(key)
		if !found {
			fmt.Println("Ne postoji element sa unetim kljucem!")

		} else {
			return value, true
		}

	} else if mt.Structure == "btree" {
		tree := mt.Data.(*BTree)
		value, found := tree.search(key)

		if !found {
			fmt.Println("Ne postoji element sa unetim kljucem!")
		} else {
			return value, true
		}

	} else if mt.Structure == "hashmap" {
		hashMap := mt.Data.(*HashMap)
		value, found := hashMap.search(key)

		if !found {
			fmt.Println("Ne postoji element sa unetim kljucem!")
		} else {
			return value, true
		}
	} else {
		log.Fatal("Nepoznata struktura memtable")
	}

	return nil, false
}

func (mt *MemoryTable) Delete(key string) {

	if mt.Structure == "skiplist" {
		sl := mt.Data.(*SkipList)
		sl.delete(key)

	} else if mt.Structure == "btree" {
		tree := mt.Data.(*BTree)
		tree.remove(key)

	} else if mt.Structure == "hashmap" {
		hashMap := mt.Data.(*HashMap)
		hashMap.delete(key)
	} else {
		log.Fatal("Nepoznata struktura memtable")
	}
}

func (mt *MemoryTable) Update(key string, value string) {
	element := Element{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().Unix(),
		Tombstone: false,
	}

	// Zatim azuriramo podatke u MemTable
	if mt.Structure == "skiplist" {
		skipList := mt.Data.(*SkipList)
		skipList.update(element)
	} else if mt.Structure == "btree" {
		bTree := mt.Data.(*BTree)
		bTree.update(element)
	} else if mt.Structure == "hashMap" {
		hashMap := mt.Data.(*HashMap)
		hashMap.update(element)
	}
}

func (mt *MemoryTable) Flush() {
	fmt.Println("Flushing memtable...")

	var elements []Element

	// Dobavljanje elemenata
	if mt.Structure == "skiplist" {
		skipList := mt.Data.(*SkipList)
		for _, elem := range skipList.getAllElements() {
			elements = append(elements, *elem)
		}
	} else if mt.Structure == "btree" {
		bTree := mt.Data.(*BTree)
		for _, elem := range bTree.getAllElements() {
			elements = append(elements, *elem)
		}
	} else if mt.Structure == "hashMap" {
		hashMap := mt.Data.(*HashMap)
		for _, elem := range hashMap.getAllElements() {
			elements = append(elements, *elem)
		}
	}

	// Sortiranje elemenata po kljucu
	sort.Slice(elements, func(i, j int) bool {
		return elements[i].Key < elements[j].Key
	})

	// Ispisivanje elemenata
	for _, element := range elements {
		fmt.Printf("Key: %s, Value: %s, Timestamp: %d, Tombstone: %t\n", element.Key, element.Value, element.Timestamp, element.Tombstone)
	}

	// Resetovanje memtable
	mt.Data = nil
	if mt.Structure == "skiplist" {
		mt.Data = newSkipList(16) // Kreiramo novu praznu SkipList
	} else if mt.Structure == "btree" {
		mt.Data = newBTree(16) // Kreiramo novu praznu BTree strukturu
	} else if mt.Structure == "hashMap" {
		mt.Data = newHashMap(16) // Kreiramo novu praznu HashMap strukturu
	}
	mt.CurrentSize = 0

	fmt.Println("Memtable flushed and reset.")
}

// func (mt *MemoryTable) IsFull() bool {
// 	switch mt.Structure {
// 	case "skiplist":
// 		return mt.Data.(*SkipList).Size() >= mt.MaxSize
// 	case "btree":
// 		// Dodaj proveru za BTree
// 	}
// 	return false
// }

func main() {
	memTable := initializeMemoryTable()

	memTable.Insert("key1", "value1")
	memTable.Insert("key2", "value2")
	memTable.Insert("key3", "value3")

	// Pretraga u Memtable
	elem, found := memTable.Search("key1")
	if found {
		fmt.Printf("Found: %s -> %s\n", elem.Key, elem.Value)
	} else {
		fmt.Println("Element not found")
	}

	memTable.Delete("key1")
	elem, found = memTable.Search("key1")
	if found {
		fmt.Printf("Found: %s -> %s\n", elem.Key, elem.Value)
	} else {
		fmt.Println("Element not found")
	}

	memTable.Update("key2", "value4")
	elem, found = memTable.Search("key2")
	if found {
		fmt.Printf("Found: %s -> %s\n", elem.Key, elem.Value)
	} else {
		fmt.Println("Element not found")
	}
}
