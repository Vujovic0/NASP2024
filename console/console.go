package console

import (
	"fmt"

	"github.com/Vujovic0/NASP2024/memtableStructures"
	"github.com/Vujovic0/NASP2024/ssTable"
	"github.com/Vujovic0/NASP2024/wal"
)

func Start() {
	blockSize := 110 // SIZE OF WAL BLOCK IN BYTES
	segmentSize := 3 // NUMBER OF BLOCKS IN SINGLE WAL FILE
	walFactory := wal.NewWAL(blockSize, segmentSize, 0, blockSize)
	memtable := memtableStructures.InitializeMemoryTable()
	fmt.Println(memtable.CurrentSize)
	if walFactory == nil {
		fmt.Println("WAL was not initialized successfully")
	} else {
		fmt.Println("WAL was inizialized successfully")
	}
	for {
		fmt.Print("--Main menu--\n 1. PUT\n 2. GET\n 3. DELETE\n 4. INFO\n 0. EXIT\n Choose one of the options above: ")
		var input int
		_, error := fmt.Scan(&input)
		if error != nil {
			fmt.Println("The input is not integer! ERROR -> ", error)
			continue
		}
		switch input {
		case 1:
			Put(walFactory)
		case 2:
			Get()
		case 3:
			Delete()
		case 4:
			fmt.Println("--ABLE FUNCTIONS--\nPUT - putting key:value pair into the program\nGET - geting the value based on the given key\nDELETE - deleting the key along side it's value")
			fmt.Println("AGREEMENT: Pair key:value from the perspective of the user are both in type string, but after the input, program restore the value into binary form.\n ...")
		case 0:
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Your input is invalid!")
		}
	}
}

func Put(walFactory *wal.WAL) (string, string) {
	fmt.Println("Enter the key: ")
	var inputKey string
	fmt.Scan(&inputKey)
	fmt.Println("Enter the value: ")
	var inputValue string
	fmt.Scan(&inputValue)
	binInputValue := stringToBin(inputValue)
	fmt.Println(binInputValue) // Writing the binary form, just for the sakes of not giving error
	// MISSING THE APPROVE FROM WAL, DATA NEED TO BE SEND TO THE WAL WERE IT WILL BE STORED TILL DISMISED TO THE DISK
	if walFactory == nil {
		fmt.Println("WAL nije uspešno inicijalizovan")
	}
	// ADDENTRY RETURN TRUE IF WAL WAS WRITTEN
	if (*walFactory).AddEntry(inputKey, inputValue) {
		fmt.Println("Uspesno unet WAL")
	} else {
		fmt.Println("Neuspesno unet WAL")
	}

	return inputKey, inputValue
}

func FindValue(inputKey string, lruCache *lruCache.LRUCache, memtableMenager *memtableStructures.MemTableManager) ([]byte, int) {
	element, found := memtableMenager.Search(inputKey)
	if found {
		lruCache.Put(inputKey, element.Value)
		return element.Value, 1
	}
	value, found := lruCache.Get(inputKey)
	if found {
		lruCache.Put(inputKey, value)
		return value, 2
	}
	valueBytes := ssTable.SearchAll([]byte(inputKey), false)
	if len(valueBytes) > 0 {
		lruCache.Put(inputKey, valueBytes)
		return valueBytes, 3
	}
	return []byte{}, 0
}

func Get(lruCache *lruCache.LRUCache, memtableMenager *memtableStructures.MemTableManager, tokenBucket *tokenBucket.TokenBucket, walFactory *wal.WAL) {
	if !tokenBucket.Consume(walFactory, memtableMenager, lruCache) {
		fmt.Println("Rate limit exceeded. Please wait before next operation.")
		return
	}
	inputKey := InputValue("Enter the key: ")
	if inputKey == "" {
		return
	}
	if inputKey == config.TokenBucketStateKey {
		fmt.Println("Error: This key is reserved for system use and cannot be accessed by users.")
		return
	}
	value, foundCase := FindValue(inputKey, lruCache, memtableMenager)
	switch foundCase {
	case 0:
		fmt.Println("There is no value for input key {" + inputKey + "}")
	case 1:
		fmt.Println("Found value {" + string(value) + "} for input key {" + inputKey + "}")
		fmt.Println("Value founded in Memtable")
	case 2:
		fmt.Println("Found value {" + string(value) + "} for input key {" + inputKey + "}")
		fmt.Println("Value founded in LRU Cache")
	case 3:
		fmt.Println("Found value {" + string(value) + "} for input key {" + inputKey + "}")
		fmt.Println("Value founded in SS Table")
	case 4:
		fmt.Println("There was an error for getting value of key {" + inputKey + "}")
	}
}

func Delete() {
	fmt.Println("Enter the key:")
	var inputKey string
	fmt.Scan(&inputKey)
	// HERE WE NEED TO GET THE VALUE BASED ON THE KEY ALONGSIDE DELETING BOTH FROM MEMORY AND DISK IF IT'S PERMANENT (?)
	// MISSING THE APPROVE FROM WAL, DATA NEED TO BE SEND TO THE WAL WERE IT WILL BE STORED TILL DISMISED TO THE DISK
	var value string
	fmt.Println("Value " + value + " with the key " + inputKey + " was deleted.")
}

func stringToBin(s string) (binString string) {
	for _, c := range s {
		binString = fmt.Sprintf("%s%b", binString, c)
	}
	return
}
