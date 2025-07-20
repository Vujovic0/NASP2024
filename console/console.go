package console

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Vujovic0/NASP2024/config"
	"github.com/Vujovic0/NASP2024/lruCache"
	"github.com/Vujovic0/NASP2024/memtableStructures"
	"github.com/Vujovic0/NASP2024/ssTable"
	"github.com/Vujovic0/NASP2024/tokenBucket"
	"github.com/Vujovic0/NASP2024/wal"
)

func hasProbabilisticPrefix(inputKey string) bool {
	if strings.HasPrefix(inputKey, config.BloomFilterPrefix) {
		return true
	}
	if strings.HasPrefix(inputKey, config.CountMinSketchPrefix) {
		return true
	}
	if strings.HasPrefix(inputKey, config.HyperLogLogPrefix) {
		return true
	}
	if strings.HasPrefix(inputKey, config.SimHashPrefix) {
		return true
	}
	return false
}

func InputValue(inputMessage string) string {
	var input string
	for {
		fmt.Println(inputMessage)
		reader := bufio.NewReader(os.Stdin)
		// CLEANING LEFTOVERS FROM LAST INPUT
		for {
			b, err := reader.Peek(1)
			if err != nil {
				break
			}
			if b[0] == '\n' || b[0] == '\r' {
				_, _ = reader.ReadByte()
			} else {
				break
			}
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			fmt.Println("Try again.")
			continue
		}
		if len(line) == 0 {
			return ""
		}
		break
	}
	return input
}

func Start() {

	config.LoadConfig()

	tokenBucket := tokenBucket.NewTokenBucket(config.TokensNum, time.Duration(config.ResetingIntervalMs)*time.Millisecond)
	lruCacheFactory := lruCache.NewLRUCache(config.CacheSize)

	walFactory := wal.NewWAL(config.GlobalBlockSize, config.BlocksInSegment)
	memtable := memtableStructures.NewMemTableManager(1)
	walFactory.LoadWALLogs(memtable)

	/*	//Small value
		inputKey := "shortKey"
		inputValue := "shortValue"
		(*walFactory).WriteLogEntry(inputKey, inputValue)
		memtable.Insert(inputKey, []byte(inputValue), walFactory.CurrentFile.Name(), walFactory.CurrentBlock, 0)

		// Medium value
		inputKey = "mediumKey"
		inputValue = strings.Repeat("x", 3000) // Still < 1 block
		(*walFactory).WriteLogEntry(inputKey, inputValue)
		memtable.Insert(inputKey, []byte(inputValue), walFactory.CurrentFile.Name(), walFactory.CurrentBlock, 0)

		// Large value (forces 2 blocks)
		inputKey = "largeKey"
		inputValue = strings.Repeat("y", 5000) // > 4096
		(*walFactory).WriteLogEntry(inputKey, inputValue)
		memtable.Insert(inputKey, []byte(inputValue), walFactory.CurrentFile.Name(), walFactory.CurrentBlock, 0)

		// Huge value (forces 3+ blocks)
		inputKey = "hugeKey"
		inputValue = strings.Repeat("z", 10000) // > 2 blocks
		(*walFactory).WriteLogEntry(inputKey, inputValue)
		memtable.Insert(inputKey, []byte(inputValue), walFactory.CurrentFile.Name(), walFactory.CurrentBlock, 0)

		inputKey = "shortKey2"
		inputValue = "shortValue2"
		(*walFactory).WriteLogEntry(inputKey, inputValue)
		memtable.Insert(inputKey, []byte(inputValue), walFactory.CurrentFile.Name(), walFactory.CurrentBlock, 0)

		inputKey = "shortKey3"
		inputValue = "shortValue3"
		(*walFactory).WriteLogEntry(inputKey, inputValue)
		memtable.Insert(inputKey, []byte(inputValue), walFactory.CurrentFile.Name(), walFactory.CurrentBlock, 0)*/

	if walFactory == nil {
		fmt.Println("WAL was not initialized successfully")
	} else {
		fmt.Println("WAL was inizialized successfully")
	}
	for {
		fmt.Print("--Main menu--\n 1. PUT\n 2. GET\n 3. DELETE\n 4. INFO\n 5. PROBABILISTIC STRUCTURES\n 0. EXIT\n Choose one of the options above: ")
		var input int
		_, error := fmt.Scan(&input)
		if error != nil {
			fmt.Println("The input is not integer! ERROR -> ", error)
			continue
		}
		switch input {
		case 1:

			Put(walFactory, memtable, lruCacheFactory, tokenBucket)
		case 2:
			Get(lruCacheFactory, memtable, tokenBucket, walFactory)
		case 3:
			Delete(walFactory, memtable, lruCacheFactory, tokenBucket)
		case 4:
			fmt.Println("--ABLE FUNCTIONS--\nPUT - putting key:value pair into the program\nGET - geting the value based on the given key\nDELETE - deleting the key along side it's value")
			fmt.Println("AGREEMENT: Pair key:value from the perspective of the user are both in type string, but after the input, program restore the value into binary form.\n ...")
		case 5:
			LoadProbabilisticConsole(walFactory, memtable, lruCacheFactory, tokenBucket)
		case 0:
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Your input is invalid!")
		}
	}
}

func PrintPrefixError() {
	fmt.Println("You entered a key with reserved prefix!")
	fmt.Println("bf_ - BloomFilter")
	fmt.Println("cms_ - CountMinSketch")
	fmt.Println("hpp_ - HyperLogLog")
	fmt.Println("sm_ - SimHash")
}

func Put(walFactory *wal.WAL, mtm *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache, tokenBucket *tokenBucket.TokenBucket) (string, string) {
	if !tokenBucket.Consume(walFactory, mtm, lruCache) {
		fmt.Println("Rate limit exceeded. Please wait before next operation.")
		return "", ""
	}
	inputKey := InputValue("Enter the key: ")
	if inputKey == "" {
		return "", ""
	}
	if inputKey == config.TokenBucketStateKey {
		fmt.Println("Error: This key is reserved for system use and cannot be accessed by users.")
		return "", ""
	}
	inputValue := InputValue("Enter the value: ")
	if inputValue == "" {
		return "", ""
	}
	if hasProbabilisticPrefix(inputKey) {
		PrintPrefixError()
		return "", ""
	}
	binInputValue := stringToBin(inputValue)
	fmt.Println(binInputValue) // Writing the binary form, just for the sakes of not giving error
	if walFactory == nil {
		fmt.Println("WAL nije uspešno inicijalizovan")
	}
	offset, err := (*walFactory).WriteLogEntry(inputKey, []byte(inputValue), false)
	if err == nil {
		mtm.Insert(inputKey, []byte(inputValue), false, walFactory.CurrentFile.Name(), walFactory.CurrentBlock, offset)
		lruCache.Put(inputKey, inputValue)
		fmt.Println("Uspesno unet WAL")
	} else {
		fmt.Println("Neuspesno unet WAL")
	}

	return inputKey, inputValue
}

func FindValue(inputKey string, lruCache *lruCache.LRUCache, memtableMenager *memtableStructures.MemTableManager) ([]byte, int) {
	element, found := memtableMenager.Search(inputKey)
	if found {
		lruCache.Put(inputKey, string(element.Value))
		return element.Value, 1
	}
	value, found := lruCache.Get(inputKey)
	if found {
		lruCache.Put(inputKey, value)
		return element.Value, 2
	}
	valueBytes := ssTable.SearchAll([]byte(inputKey), false)
	if len(valueBytes) > 0 {
		lruCache.Put(inputKey, value)
		return element.Value, 3
	}
	return []byte(""), 0
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

func Delete(walFactory *wal.WAL, mtm *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache, tokenBucket *tokenBucket.TokenBucket) {
	if !tokenBucket.Consume(walFactory, mtm, lruCache) {
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
	// HERE WE NEED TO GET THE VALUE BASED ON THE KEY ALONGSIDE DELETING BOTH FROM MEMORY AND DISK IF IT'S PERMANENT (?)
	// MISSING THE APPROVE FROM WAL, DATA NEED TO BE SEND TO THE WAL WERE IT WILL BE STORED TILL DISMISED TO THE DISK
	offset, err := (*walFactory).WriteLogEntry(inputKey, []byte(""), false)
	if err == nil {
		mtm.Insert(inputKey, []byte(""), true, walFactory.CurrentFile.Name(), walFactory.CurrentBlock, offset)
		//lruCache.Put(inputKey, inputValue)
		fmt.Println("Uspesno unet WAL")
	} else {
		fmt.Println("Neuspesno unet WAL")
	}
	lruCache.Remove(inputKey)
	fmt.Println("Log with key {" + inputKey + "} is deleted.")
}

func stringToBin(s string) (binString string) {
	for _, c := range s {
		binString = fmt.Sprintf("%s%b", binString, c)
	}
	return
}
