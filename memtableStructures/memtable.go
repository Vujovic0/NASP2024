package memtableStructures

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"sort"
	"time"

	"github.com/Vujovic0/NASP2024/config"
)

type Config struct {
	WalSize           uint64 `json:"wal_size"`
	MemtableSize      uint64 `json:"memtable_size"`
	MemtableStructure string `json:"memtable_structure"`
}

type MemoryTable struct {
	Data           interface{} // Moze biti SkipList, BTree ili HashMap
	MaxSize        uint64
	Structure      string // Moze biti "btree","skiplist" ili "hashMap"
	CurrentSize    int
	WALLastSegment string // NAME OF THE LAST WAL USED FOR THIS TABLE, NEEDED FOR THE WAL LOADING
	WALBlockOffset int    // BLOCK INDEX IN SEGMENT
	WALByteOffset  int    // BYTE OFFSET, NEEDED ONLY FOR BLOCK WHICH TYPE != 0
}

type Element struct {
	Key       string
	Value     []byte
	Timestamp int64
	Tombstone bool
}

func loadConfig() Config {
	// Postavi default vrednosti
	config := Config{
		WalSize:           10,
		MemtableSize:      10,
		MemtableStructure: "hashMap",
	}

	// Pokusavamo da ucitamo konfiguraciju iz fajla
	configData, err := os.ReadFile("config.json")
	if err == nil {
		json.Unmarshal(configData, &config)
		// Ako neko polje nedostaje u fajlu, ostaje default iz koda!
	}
	return config
}

func initializeMemoryTable() *MemoryTable {
	var config Config
	configData, err := os.ReadFile("./memtableStructures/config.json")
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
	case "hashMap":
		memTable = initializeHashMapMemtable(&config)
	default:
		log.Fatal("Nepoznata struktura memtable: ", config.MemtableStructure)
	}

	return memTable
}

// Metode koje moraju imati skiplist i btree
type Memtable interface {
	Insert(key string, value []byte, timestamp int64, tombstone bool)
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

func (mt *MemoryTable) Insert(key string, value []byte) {
	element := Element{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().Unix(),
		Tombstone: false,
	}

	fmt.Printf("Inserting element: Key=%s, Value=%s\n", key, value)

	if mt.Structure == "skiplist" {
		skipList := mt.Data.(*SkipList)
		skipList.insert(element.Key, element.Value, element.Timestamp, element.Tombstone)
		mt.CurrentSize++
		// if mt.CurrentSize >= int(mt.MaxSize) {
		// 	mt.Flush()
		// }
	} else if mt.Structure == "btree" {
		bTree := mt.Data.(*BTree)
		bTree.insert(element)

		mt.CurrentSize++
		// if mt.CurrentSize >= int(mt.MaxSize) {
		// 	mt.Flush()
		// }
	} else if mt.Structure == "hashMap" {
		hashMap := mt.Data.(*HashMap)
		hashMap.insert(element)

		mt.CurrentSize++
		// if mt.CurrentSize >= int(mt.MaxSize) {
		// 	mt.Flush()
		// }

	} else {
		log.Fatal("Nepoznata struktura memtable")
	}

}

func (mt *MemoryTable) Search(key string) (*Element, bool) {

	if mt.Structure == "skiplist" {
		sl := mt.Data.(*SkipList)
		value, found := sl.search(key)
		if !found {
			//fmt.Println("Ne postoji element sa unetim kljucem!")

		} else {
			return value, true
		}

	} else if mt.Structure == "btree" {
		tree := mt.Data.(*BTree)
		value, found := tree.search(key)

		if !found {
			//fmt.Println("Ne postoji element sa unetim kljucem!")
		} else {
			return value, true
		}

	} else if mt.Structure == "hashMap" {
		hashMap := mt.Data.(*HashMap)
		value, found := hashMap.search(key)

		if !found {
			//fmt.Println("Ne postoji element sa unetim kljucem!")
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

	} else if mt.Structure == "hashMap" {
		hashMap := mt.Data.(*HashMap)
		hashMap.delete(key)
	} else {
		log.Fatal("Nepoznata struktura memtable")
	}
}

func (mt *MemoryTable) Update(key string, value []byte) {
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

func (mt *MemoryTable) Flush() []Element {
	// fmt.Println("Flushing memtable...")

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
	// for _, element := range elements {
	// 	fmt.Printf("Key: %s, Value: %s, Timestamp: %d, Tombstone: %t\n", element.Key, element.Value, element.Timestamp, element.Tombstone)
	// }

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
	return elements
}

func (mt *MemoryTable) toBytes() []byte {
	var buffer bytes.Buffer

	if mt.Structure == "skiplist" {
		sl := mt.Data.(*SkipList)
		for _, element := range sl.getAllElements() {
			elementBytes := elementToBytes(element)
			fmt.Printf("Serialized Element Bytes (Hex): %x\n", elementBytes)
			buffer.Write(elementBytes)
		}
	} else if mt.Structure == "btree" {
		bt := mt.Data.(*BTree)
		for _, element := range bt.getAllElements() {
			elementBytes := elementToBytes(element)
			buffer.Write(elementBytes)
		}
	} else if mt.Structure == "hashMap" {
		hm := mt.Data.(*HashMap)
		for _, element := range hm.getAllElements() {
			elementBytes := elementToBytes(element)
			buffer.Write(elementBytes)
		}
	}

	// Racunamo CRC

	crc := crc32.ChecksumIEEE(buffer.Bytes())
	binary.Write(&buffer, binary.LittleEndian, uint32(crc))

	return buffer.Bytes()
}

func (mt *MemoryTable) lastElementToBytes() []byte {
	var lastElement *Element

	if mt.Structure == "skiplist" {
		sl := mt.Data.(*SkipList)
		lastElement = sl.LastElement()
	} else if mt.Structure == "btree" {
		bt := mt.Data.(*BTree)
		lastElement = bt.LastElement()
	} else if mt.Structure == "hashMap" {
		hm := mt.Data.(*HashMap)
		lastElement = hm.LastElement()
	}

	return elementToBytes(lastElement)
}

func readNextElement(buffer *bytes.Reader, originalData []byte) []byte {
	elementBuffer := new(bytes.Buffer)
	tmp := make([]byte, binary.MaxVarintLen64)

	// 1. Timestamp
	timestamp, _ := binary.ReadVarint(buffer)
	n := binary.PutVarint(tmp, timestamp)
	elementBuffer.Write(tmp[:n])

	// 2. Tombstone (1 bajt)
	tombstoneByte, _ := buffer.ReadByte()
	elementBuffer.WriteByte(tombstoneByte)
	tombstone := tombstoneByte != 0

	// 3. Duzina kljuca
	keyLen, _ := binary.ReadUvarint(buffer)
	n = binary.PutUvarint(tmp, keyLen)
	elementBuffer.Write(tmp[:n])

	// 4. Valuesize (samo ako nije tombstone)
	var valueLen uint64
	if !tombstone {
		valueLen, _ = binary.ReadUvarint(buffer)
		n = binary.PutUvarint(tmp, valueLen)
		elementBuffer.Write(tmp[:n])
	}

	// 5. Kljuc
	key := make([]byte, keyLen)
	buffer.Read(key)
	elementBuffer.Write(key)

	// 6. Vrednost (samo ako nije tombstone)
	if !tombstone {
		value := make([]byte, valueLen)
		buffer.Read(value)
		elementBuffer.Write(value)
	}

	return elementBuffer.Bytes()
}

func elementToBytes(element *Element) []byte {
	var buffer bytes.Buffer
	tmp := make([]byte, binary.MaxVarintLen64)

	// 1. Timestamp
	if config.VariableEncoding {
		n := binary.PutVarint(tmp, element.Timestamp)
		buffer.Write(tmp[:n])
	} else {
		timestampBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(timestampBytes, uint64(element.Timestamp))
		buffer.Write(timestampBytes)
	}

	// 2. Tombstone
	if element.Tombstone {
		buffer.WriteByte(1)
	} else {
		buffer.WriteByte(0)
	}

	// 3. Duzina kljuca
	if config.VariableEncoding {
		n := binary.PutUvarint(tmp, uint64(len(element.Key)))
		buffer.Write(tmp[:n])
	} else {
		keyLen := make([]byte, 8)
		binary.LittleEndian.PutUint64(keyLen, uint64(len(element.Key)))
		buffer.Write(keyLen)
	}

	// 4. Ako element nije tombstone, upisujemo duzinu vrednosti
	if !element.Tombstone {
		if config.VariableEncoding {
			n := binary.PutUvarint(tmp, uint64(len(element.Value)))
			buffer.Write(tmp[:n])
		} else {
			valueLen := make([]byte, 8)
			binary.LittleEndian.PutUint64(valueLen, uint64(len(element.Value)))
			buffer.Write(valueLen)
		}
	}

	// 5. Kljuc
	buffer.WriteString(element.Key)

	// 6. Value (samo ako nije tombstone)
	if !element.Tombstone {
		buffer.Write(element.Value)
	}

	return buffer.Bytes()
}

func bytesToElement(data []byte) *Element {
	buffer := bytes.NewReader(data)

	// 1. Timestamp
	var timestamp int64
	if config.VariableEncoding {
		timestamp, _ = binary.ReadVarint(buffer)
	} else {
		timestampBytes := make([]byte, 8)
		buffer.Read(timestampBytes)
		timestamp = int64(binary.LittleEndian.Uint64(timestampBytes))
	}

	// 2. Tombstone (1 bajt)
	tombstoneByte, _ := buffer.ReadByte()
	tombstone := tombstoneByte != 0

	// 3. Duzina kljuca
	var keyLen uint64
	if config.VariableEncoding {
		keyLen, _ = binary.ReadUvarint(buffer)
	} else {
		keyLenBytes := make([]byte, 8)
		buffer.Read(keyLenBytes)
		keyLen = binary.LittleEndian.Uint64(keyLenBytes)
	}

	// 4. Valuesize (samo ako nije tombstone)
	var valueLen uint64
	if !tombstone {
		if config.VariableEncoding {
			valueLen, _ = binary.ReadUvarint(buffer)
		} else {
			valueLenBytes := make([]byte, 8)
			buffer.Read(valueLenBytes)
			valueLen = binary.LittleEndian.Uint64(valueLenBytes)
		}
	}

	// 5. Kljuc
	key := make([]byte, keyLen)
	buffer.Read(key)

	// 6. Vrednost (samo ako nije tombstone)
	var value []byte
	if !tombstone {
		value = make([]byte, valueLen)
		buffer.Read(value)
	}

	return &Element{
		Key:       string(key),
		Value:     value,
		Timestamp: timestamp,
		Tombstone: tombstone,
	}
}

func verifyCRC(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	crcData := data[:len(data)-4]
	expectedCRC := binary.LittleEndian.Uint32(data[len(data)-4:])
	actualCRC := crc32.ChecksumIEEE(crcData)
	return expectedCRC == actualCRC
}

func (mt *MemoryTable) SearchByPrefix(prefix string) ([]*Element, bool) {
	var results []*Element

	if mt.Structure == "skiplist" {
		sl := mt.Data.(*SkipList)
		results = sl.searchByPrefix(prefix)
	} else if mt.Structure == "btree" {
		bt := mt.Data.(*BTree)
		results = bt.searchByPrefix(prefix)
	} else if mt.Structure == "hashMap" {
		hm := mt.Data.(*HashMap)
		results = hm.searchByPrefix(prefix)
	} else {
		log.Fatal("Nepoznata struktura memtable")
	}

	return results, len(results) > 0
}

func (mt *MemoryTable) Reset() {
	// Resetovanje MemTable-a
	if mt.Structure == "skiplist" {
		mt.Data = newSkipList(16)
	} else if mt.Structure == "btree" {
		mt.Data = newBTree(16)
	} else if mt.Structure == "hashMap" {
		mt.Data = newHashMap(16)
	}
	mt.CurrentSize = 0
}

func (mt *MemoryTable) SearchByRange(startKey, endKey string) ([]*Element, bool) {
	var results []*Element

	if mt.Structure == "skiplist" {
		sl := mt.Data.(*SkipList)
		results = sl.searchByRange(startKey, endKey)
	} else if mt.Structure == "btree" {
		bt := mt.Data.(*BTree)
		results = bt.searchByRange(startKey, endKey)
	} else if mt.Structure == "hashMap" {
		hm := mt.Data.(*HashMap)
		results = hm.searchByRange(startKey, endKey)
	} else {
		log.Fatal("Nepoznata struktura memtable")
	}

	return results, len(results) > 0
}

// func main() {
// 	memTable := initializeMemoryTable()

// 	// memTable.Insert("key1", "value1")
// 	memTable.Insert("key2", []byte("value2"))
// 	memTable.Insert("key3", []byte("value3"))
// 	memTable.Insert("key4", []byte("value4"))

// 	// Pretraga u Memtable
// 	elem, found := memTable.Search("key1")
// 	if found {
// 		fmt.Printf("Found: %s -> %s\n", elem.Key, elem.Value)
// 	} else {
// 		fmt.Println("Element not found")
// 	}

// 	memTable.Delete("key3")
// 	elem, found = memTable.Search("key2")
// 	if found {
// 		fmt.Printf("Found: %s -> %s\n", elem.Key, elem.Value)
// 	} else {
// 		fmt.Println("Element not found")
// 	}

// 	memTable.Update("key2", []byte("value4"))
// 	elem, found = memTable.Search("key2")
// 	if found {
// 		fmt.Printf("Found: %s -> %s\n", elem.Key, elem.Value)
// 	} else {
// 		fmt.Println("Element not found")
// 	}

// 	// Serijalizacija MemTable
// 	data := memTable.toBytes()

// 	fmt.Println("Ispis !!!!!!!!!!!!!")

// 	// Ispis rezultata
// 	fmt.Printf("Serialized MemTable to bytes: %x\n", data)
// 	fmt.Printf("Length of serialized data: %d bytes\n", len(data))

// 	buffer := bytes.NewReader(data[:len(data)-4]) // Ignorisemo poslednja 4 bajta (CRC)
// 	for buffer.Len() > 0 {
// 		// Deserializacija elementa
// 		element := bytesToElement(readNextElement(buffer, data))
// 		fmt.Printf("Deserialized Element: Key=%s, Value=%s, Timestamp=%d, Tombstone=%t\n",
// 			element.Key, element.Value, element.Timestamp, element.Tombstone)
// 	}

// 	if verifyCRC(data) {
// 		fmt.Println("CRC is valid!")
// 	} else {
// 		fmt.Println("CRC is invalid!")
// 	}
// }
