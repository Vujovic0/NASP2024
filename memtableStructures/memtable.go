package memtableStructures

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

type Config struct { // CONFIGURATION LOADED FROM CONFIGURATION.JSON
	WalSize           uint64 `json:"wal_size"`
	MemtableSize      uint64 `json:"memtable_size"`
	MemtableStructure string `json:"memtable_structure"`
}

type MemoryTable struct { // MEMTABLE
	Data        interface{} // Moze biti SkipList ili BTree
	MaxSize     uint64
	Structure   string // Moze biti "btree" ili "skiplist"
	CurrentSize int
}

type Element struct { // ELEMENT IN MEMTABLE
	Key       string
	Value     string
	Timestamp int64
	Tombstone bool
}

func InitializeMemoryTable() *MemoryTable {
	var config Config
	configData, err := os.ReadFile("configuration.json")
	if err != nil {
		log.Fatal(err)
	}
	err = json.Unmarshal(configData, &config)
	if err != nil {
		log.Fatal("Error parsing config: ", err)
	}
	fmt.Println(config)

	// memTable := &MemoryTable{
	// 	Data:      make(map[string]string),
	// 	MaxSize:   config.MemtableSize,
	// 	Structure: config.MemtableStructure,
	// }
	// fmt.Println("Memtable initialized with config: ", config)

	var memTable *MemoryTable

	switch config.MemtableStructure {
	case "btree":
		memTable = initializeBTreeMemTable(&config)
	case "skiplist":
		memTable = initializeSkipListMemTable(&config)
	default:
		log.Fatal("Nepoznata struktura memtable: ", config.MemtableStructure)
	}

	return memTable
}

// func initializeMemTable() *MemoryTable {
// 	var config Config
// 	configData, err := os.ReadFile("config (1).json")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	err = json.Unmarshal(configData, &config)
// 	if err != nil {
// 		log.Fatal("Error parsing config: ", err)
// 	}
// 	fmt.Println(config)

// 	switch config.MemtableStructure {
// 	case "btree":
// 		return initializeSkipListMemTable(&config)
// 	case "skiplist":
// 		return initializeTreeMemTable(&config)
// 	default:
// 		log.Fatal("Nepoznata struktura memtable: ", config.MemtableStructure)
// 		return nil
// 	}
// }

// Metode koje moraju imati skiplist i btree
type Memtable interface {
	Insert(key string, value string, timestamp int64, tombstone bool)
	Search(key string) (*Element, bool)
	Delete(key string)
}

func initializeSkipListMemTable(config *Config) *MemoryTable {
	// Kreiranje SkipList-a
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
	// ovde treba kreirati BTree

	memTable := &MemoryTable{
		Data:        nil, // ovde treba BTree struktura
		MaxSize:     config.MemtableSize,
		Structure:   config.MemtableStructure,
		CurrentSize: 0,
	}
	fmt.Println("Memtable initialized with config: ", config)

	return memTable
}

func (mt *MemoryTable) Insert(key string, value string) {
	if mt.Structure == "skiplist" {
		skipList := mt.Data.(*SkipList)
		skipList.insert(key, value, time.Now().Unix(), false)
		mt.CurrentSize += 1
		if mt.CurrentSize >= int(mt.MaxSize) {
			mt.Flush()
		}
	} else if mt.Structure == "btree" {
		// bTree := mt.Data.(*BTree) // Pretpostavljamo da imamo BTree strukturu
		// bTree.insert(key, value)
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
		value := sl.search(key)
		if value == nil {
			fmt.Println("Ne postoji element sa unetim kljucem!")

		} else {
			return value, true
		}

	} else if mt.Structure == "btree" {
		// Pretpostavljamo da imamo BTree strukturu

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
		// Pretpostavljamo da imamo BTree strukturu

	} else {
		log.Fatal("Nepoznata struktura memtable")
	}
}

func (s *SkipList) printAll() {
	current := s.head.next[0] // Pocinjemo od najnizeg nivoa
	for current != nil {
		if !current.value.Tombstone { // Preskacemo logicki obrisane elemente
			fmt.Printf("Key: %s, Value: %s, Timestamp: %d\n", current.value.Key, current.value.Value, current.value.Timestamp)
		}
		current = current.next[0]
	}
}

func (mt *MemoryTable) Flush() {
	fmt.Println("Flushing memtable...")

	// Ispis sortirane liste podataka
	if mt.Structure == "skiplist" {
		skipList := mt.Data.(*SkipList)
		skipList.printAll() // Dodajemo funkciju za ispis svih elemenata
	} else if mt.Structure == "btree" {
		// BTree implementacija - treba dodati inorder traversal
		fmt.Println("BTree flush is not yet implemented.")
	} else {
		log.Fatal("Nepoznata struktura memtable")
	}

	// Resetovanje memtable
	mt.Data = nil
	if mt.Structure == "skiplist" {
		mt.Data = newSkipList(16) // Kreiramo novu praznu SkipList
	} else if mt.Structure == "btree" {
		// Kreirajte novu praznu BTree strukturu
		mt.Data = nil
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

/* func main() {
	// var config Config
	// configData, err := os.ReadFile("config (1).json")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// json.Unmarshal(configData, &config)
	// fmt.Println(config)
	// marshalled, err := json.Marshal(config)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println(string(marshalled))

	// memTable := initializeMemoryTable()
	// fmt.Println(memTable)

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
}
 */