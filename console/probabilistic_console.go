package console

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/Vujovic0/NASP2024/config"
	"github.com/Vujovic0/NASP2024/lruCache"
	"github.com/Vujovic0/NASP2024/memtableStructures"
	"github.com/Vujovic0/NASP2024/probabilisticDataStructures/bloomFilter"
	"github.com/Vujovic0/NASP2024/probabilisticDataStructures/cms"
	"github.com/Vujovic0/NASP2024/probabilisticDataStructures/hyperloglog"
	"github.com/Vujovic0/NASP2024/simhash"
	"github.com/Vujovic0/NASP2024/tokenBucket"
	"github.com/Vujovic0/NASP2024/wal"
)

func AddPrefix(typeInput int, inputName string) string {
	switch typeInput {
	case 1:
		return config.BloomFilterPrefix + inputName
	case 2:
		return config.CountMinSketchPrefix + inputName
	case 3:
		return config.HyperLogLogPrefix + inputName
	case 4:
		return config.SimHashPrefix + inputName
	}
	// DODAJ NEKI BOLJI RETURN
	return ""
}

func BloomFilterParametersInput() (int, float64) {
	var elementsNum int
	var falsePositive float64
	for {
		fmt.Println("Enter the expected number of elements: ")
		_, error := fmt.Scan(&elementsNum)
		if error != nil {
			fmt.Println("You need to input an integer!")
			continue
		}
		if elementsNum <= 0 {
			fmt.Println("You need to input an integer > 0!")
			continue
		}
		break
	}
	for {
		fmt.Println("Enter the false positive percentege: ")
		_, error := fmt.Scan(&falsePositive)
		if error != nil {
			fmt.Println("You need to input a float number!")
			continue
		}
		if falsePositive <= 0 || falsePositive > 1 {
			fmt.Println("You need to input an 0 < float < 1!")
			continue
		}
		break
	}
	return elementsNum, falsePositive
}

func CountMinSketchParametersInput() (float64, float64) {
	var epsilon float64
	var delta float64
	for {
		fmt.Println("Enter the epsilon: ")
		_, error := fmt.Scan(&epsilon)
		if error != nil {
			fmt.Println("You need to input a float number!")
			continue
		}
		if epsilon <= 0 {
			fmt.Println("You need to input a float > 0!")
			continue
		}
		break
	}
	for {
		fmt.Println("Enter the delta: ")
		_, error := fmt.Scan(&delta)
		if error != nil {
			fmt.Println("You need to input a float number!")
			continue
		}
		if delta <= 0 || delta > 1 {
			fmt.Println("You need to input an 0 < float < 1!")
			continue
		}
		break
	}
	return epsilon, delta
}

func HyperLogLogParametersInput() int {
	var p int
	for {
		fmt.Println("Enter the precision between 4 and 16: ")
		_, error := fmt.Scan(&p)
		if error != nil {
			fmt.Println("You need to input an integer number!")
			continue
		}
		if p < 4 || p > 16 {
			fmt.Println("You need to input 4 < integer < 16!")
			continue
		}
		break
	}
	return p
}

func CreateNewInstance(typeInput int, wal *wal.WAL, memtable *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache, tokenBucket *tokenBucket.TokenBucket) {
	if !tokenBucket.Consume(wal, memtable, lruCache) {
		fmt.Println("Rate limit exceeded. Please wait before next operation.")
		return
	}
	var instanceName string
	instanceName = InputValue("Enter the name of new instance: ")
	if instanceName == "" {
		return
	}
	if hasProbabilisticPrefix(instanceName) {
		PrintPrefixError()
		return
	}
	instanceName = AddPrefix(typeInput, instanceName)
	switch typeInput {
	case 1:
		elementsNum, falsePositive := BloomFilterParametersInput()
		bf := bloomFilter.MakeBloomFilter(elementsNum, falsePositive)
		bfBytes, err := bloomFilter.SerializeToBytes(bf)
		if err != nil {
			fmt.Println("Error happend while serializing BloomFilter! Returning...")
			return
		}
		offset, err := wal.WriteLogEntry(instanceName, bfBytes, false)
		if err != nil {
			fmt.Println("Error happend while writing WAL! Returning...")
			return
		}
		memtable.Insert(instanceName, bfBytes, false, wal.CurrentFile.Name(), wal.CurrentBlock, offset)
		fmt.Println("New instance of BloomFilter saved...")
		return
	case 2:
		epsilon, delta := CountMinSketchParametersInput()
		cmsObject := cms.MakeCountMinSketch(nil, epsilon, delta)
		cmsBytes, err := cms.SerializeToBytes(cmsObject)
		if err != nil {
			fmt.Println("Error happend while serializing CountMinSketch! Returning...")
			return
		}
		offset, err := wal.WriteLogEntry(instanceName, cmsBytes, false)
		if err != nil {
			fmt.Println("Error happend while writing WAL! Returning...")
			return
		}
		memtable.Insert(instanceName, cmsBytes, false, wal.CurrentFile.Name(), wal.CurrentBlock, offset)
		//lruCache.Put()
		fmt.Println("New instance of BloCountMinSketchomFilter saved...")
		return
	case 3:
		p := HyperLogLogParametersInput()
		hll := hyperloglog.MakeHyperLogLog(uint8(p))
		hllBytes, err := hyperloglog.SerializeToBytes(hll)
		if err != nil {
			fmt.Println("Error happend while serializing HyperLogLog! Returning...")
			return
		}
		offset, err := wal.WriteLogEntry(instanceName, hllBytes, false)
		if err != nil {
			fmt.Println("Error happend while writing WAL! Returning...")
			return
		}
		memtable.Insert(instanceName, hllBytes, false, wal.CurrentFile.Name(), wal.CurrentBlock, offset)
		fmt.Println("New instance of HyperLogLog saved...")
		return
	}
}

func DeleteExistingInstance(typeInput int, wal *wal.WAL, memtable *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache, tokenBucket *tokenBucket.TokenBucket) {
	if !tokenBucket.Consume(wal, memtable, lruCache) {
		fmt.Println("Rate limit exceeded. Please wait before next operation.")
		return
	}
	var instanceName string
	instanceName = InputValue("Enter the name of instance you want to delete: ")
	if instanceName == "" {
		return
	}
	if hasProbabilisticPrefix(instanceName) {
		PrintPrefixError()
		return
	}
	instanceName = AddPrefix(typeInput, instanceName)
	offset, err := wal.WriteLogEntry(instanceName, []byte(""), true)
	if err != nil {
		fmt.Println("Error happend while writing WAL! Returning...")
		return
	}
	memtable.Insert(instanceName, []byte(""), true, wal.CurrentFile.Name(), wal.CurrentBlock, offset)
	lruCache.Remove(instanceName)
	switch typeInput {
	case 1:
		fmt.Println("Instance of BloomFilter deleted...")
	case 2:
		fmt.Println("Instance of CountMinSketch deleted...")
	case 3:
		fmt.Println("Instance of HyperLogLog deleted...")
	case 4:
		fmt.Println("Instance of SimHash deleted...")
	}
}

func ReadInputValues() []string {
	var stringovi []string
	for {
		fmt.Print("Enter a value you want to input: ")
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
			return nil
		}
		if len(line) == 0 {
			break
		}
		stringovi = append(stringovi, strings.TrimRight(line, "\r\n"))
	}
	return stringovi
}

func AddElements(typeInput int, wal *wal.WAL, memtable *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache, tokenBucket *tokenBucket.TokenBucket) {
	if !tokenBucket.Consume(wal, memtable, lruCache) {
		fmt.Println("Rate limit exceeded. Please wait before next operation.")
		return
	}
	var instanceName string
	instanceName = InputValue("Enter the name of instance you want to access: ")
	if instanceName == "" {
		return
	}
	if hasProbabilisticPrefix(instanceName) {
		PrintPrefixError()
		return
	}
	instanceName = AddPrefix(typeInput, instanceName)

	foundBytes, foundCase := FindValue(instanceName, lruCache, memtable)
	if foundCase == 0 {
		fmt.Println("Instance with that input name doesn't exist!")
		return
	}

	inputValues := ReadInputValues()

	switch typeInput {
	case 1:
		bf, err := bloomFilter.DeserializeFromBytes(foundBytes)
		if err != nil {
			fmt.Println("Error while deserializing instance of BloomFilter!")
			return
		}
		bf = bloomFilter.AddData(bf, inputValues)
		bfBytes, err := bloomFilter.SerializeToBytes(bf)
		if err != nil {
			fmt.Println("Error happend while serializing BloomFilter! Returning...")
			return
		}
		offset, err := wal.WriteLogEntry(instanceName, bfBytes, false)
		if err != nil {
			fmt.Println("Error happend while writing WAL! Returning...")
			return
		}
		memtable.Insert(instanceName, bfBytes, false, wal.CurrentFile.Name(), wal.CurrentBlock, offset)
		fmt.Println("Updated instance of BloomFilter saved...")
		return
	case 2:
		cmsObject, err := cms.DeserializeFromBytes(foundBytes)
		if err != nil {
			fmt.Println("Error while deserializing instance of CountMinSketch!")
			return
		}
		cmsObject = cms.UpdateCountMinSketch(cmsObject, inputValues)
		cmsBytes, err := cms.SerializeToBytes(cmsObject)
		if err != nil {
			fmt.Println("Error happend while serializing CountMinSketch! Returning...")
			return
		}
		offset, err := wal.WriteLogEntry(instanceName, cmsBytes, false)
		if err != nil {
			fmt.Println("Error happend while writing WAL! Returning...")
			return
		}
		memtable.Insert(instanceName, cmsBytes, false, wal.CurrentFile.Name(), wal.CurrentBlock, offset)
		fmt.Println("Updated instance of CountMinSketch saved...")
		return
	case 3:
		hll, err := hyperloglog.DeserializeFromBytes(foundBytes)
		if err != nil {
			fmt.Println("Error while deserializing instance of HyperLogLog!")
			return
		}
		hll = hyperloglog.UpdateHyperLogLog(hll, inputValues)
		hllBytes, err := hyperloglog.SerializeToBytes(hll)
		if err != nil {
			fmt.Println("Error happend while serializing HyperLogLog! Returning...")
			return
		}
		offset, err := wal.WriteLogEntry(instanceName, hllBytes, false)
		if err != nil {
			fmt.Println("Error happend while writing WAL! Returning...")
			return
		}
		memtable.Insert(instanceName, hllBytes, false, wal.CurrentFile.Name(), wal.CurrentBlock, offset)
		fmt.Println("Updated instance of HyperLogLog saved...")
		return
	}

}

func BloomFilterSpecific(memtable *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache) {
	var instanceName string
	instanceName = InputValue("Enter the name of instance you want to access: ")
	if instanceName == "" {
		return
	}
	if hasProbabilisticPrefix(instanceName) {
		PrintPrefixError()
		return
	}
	instanceName = AddPrefix(1, instanceName)
	foundBytes, found := FindValue(instanceName, lruCache, memtable)
	if found == 0 {
		fmt.Println("Coulnd't find BloomFilter instance with provided name!")
		return
	}
	bf, err := bloomFilter.DeserializeFromBytes(foundBytes)
	if err != nil {
		fmt.Println("Error while loading BloomFilter instance!")
		return
	}
	var inputValue string
	for {
		inputValue = InputValue("Enter value you want check in BloomFilter: ")
		if inputValue == "" {
			return
		}
		found := bloomFilter.SearchData(bf, inputValue)
		if found {
			fmt.Println("Value {" + inputValue + "} is found in {" + instanceName + "} instance of BloomFilter")
		} else {
			fmt.Println("Value {" + inputValue + "} is not found in {" + instanceName + "} instance of BloomFilter")
		}
		return
	}
}

func CMSSpecific(memtable *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache) {
	var instanceName string
	instanceName = InputValue("Enter the name of instance you want to access: ")
	if instanceName == "" {
		return
	}
	if hasProbabilisticPrefix(instanceName) {
		PrintPrefixError()
		return
	}
	instanceName = AddPrefix(2, instanceName)
	foundBytes, found := FindValue(instanceName, lruCache, memtable)
	if found == 0 {
		fmt.Println("Coulnd't find BloomFilter instance with provided name!")
		return
	}
	cmsObject, err := cms.DeserializeFromBytes(foundBytes)
	if err != nil {
		fmt.Println("Error while loading CountMinSketch instance!")
		return
	}
	var inputValue string
	for {
		inputValue = InputValue("Enter value you want check in CountMinSketch: ")
		if inputValue == "" {
			return
		}
		count := cms.SearchData(cmsObject, inputValue)
		if count != 0 {
			fmt.Printf("Value {"+inputValue+"} is found in {"+instanceName+"} instance of CountMinSketch {%d} times\n", count)
		} else {
			fmt.Println("Value {" + inputValue + "} is not found in {" + instanceName + "} instance of CountMinSketch")
		}
		return
	}
}

func HLLSpecific(memtable *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache) {
	var instanceName string
	instanceName = InputValue("Enter the name of instance you want to access: ")
	if instanceName == "" {
		return
	}
	if hasProbabilisticPrefix(instanceName) {
		PrintPrefixError()
		return
	}
	instanceName = AddPrefix(3, instanceName)
	foundBytes, found := FindValue(instanceName, lruCache, memtable)
	if found == 0 {
		fmt.Println("Coulnd't find HyperLogLog instance with provided name!")
		return
	}
	hll, err := hyperloglog.DeserializeFromBytes(foundBytes)
	if err != nil {
		fmt.Println("Error while loading HyperLogLog instance!")
		return
	}
	cardinality := hyperloglog.GetNumberOfDifferentValues(hll)
	fmt.Printf("Cardinality of HyperLogLog instance {"+instanceName+"} is {%d}\n", cardinality)
}

func SpecificOperation(typeInput int, memtable *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache) {
	switch typeInput {
	case 1:
		BloomFilterSpecific(memtable, lruCache)
	case 2:
		CMSSpecific(memtable, lruCache)
	case 3:
		HLLSpecific(memtable, lruCache)
	}
}

func OperationsMenu(typeInput int, wal *wal.WAL, memtable *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache, tokenBucket *tokenBucket.TokenBucket) {
	var typeName string
	var specificOperation string
	switch typeInput {
	case 1:
		typeName = "BloomFilter"
		specificOperation = "Provera prisutnosti elementa"
	case 2:
		typeName = "CountMinSketch"
		specificOperation = "Provera ucestalosti dogadjaja"
	case 3:
		typeName = "HyperLogLog"
		specificOperation = "Provera kardinaliteta"
	}
	for {
		fmt.Print("--" + typeName + " operations menu--\n 1. NEW INSTANCE\n 2. DELETE EXISTING INSTANCE\n 3. ADD NEW ELEMENT\n 4. " + specificOperation + "\n 0. EXIT\n Choose one of the options above: ")
		var operationInput int
		_, error := fmt.Scan(&operationInput)
		if error != nil {
			fmt.Println("The input is not integer! ERROR -> ", error)
			continue
		}
		switch operationInput {
		case 1:
			CreateNewInstance(typeInput, wal, memtable, lruCache, tokenBucket)
		case 2:
			DeleteExistingInstance(typeInput, wal, memtable, lruCache, tokenBucket)
		case 3:
			AddElements(typeInput, wal, memtable, lruCache, tokenBucket)
		case 4:
			SpecificOperation(typeInput, memtable, lruCache)
			// OPERACIJA SPECIFIČNA TIPU
		case 0:
			fmt.Println("Returning to probabilistic menu...")
			return
		default:
			fmt.Println("Your input is invalid!")
		}
	}

}

func SimHashAddElement(wal *wal.WAL, memtable *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache, tokenBucket *tokenBucket.TokenBucket) {
	if !tokenBucket.Consume(wal, memtable, lruCache) {
		fmt.Println("Rate limit exceeded. Please wait before next operation.")
		return
	}
	var instanceName string
	instanceName = InputValue("Enter the name of new SimHash instance: ")
	if instanceName == "" {
		return
	}
	if hasProbabilisticPrefix(instanceName) {
		PrintPrefixError()
		return
	}
	instanceName = AddPrefix(4, instanceName)

	var textValue string
	textValue = InputValue("Enter the text of new SimHash instance: ")
	if textValue == "" {
		return
	}
	simHashBytes := simhash.GetFingerPrint(textValue)
	offset, err := wal.WriteLogEntry(instanceName, simHashBytes[:], false)
	if err != nil {
		fmt.Println("There was an error while writing WAL!")
		return
	}
	memtable.Insert(instanceName, simHashBytes[:], false, wal.CurrentFile.Name(), wal.CurrentBlock, offset)
	// DODAJ DA SE ZAPIS DODAJE U LRU CACHE
	fmt.Println("Saving SimHash instance successful.")
}

func SimHashCompareDistance(memtable *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache) {
	var firstRecord string
	firstRecord = InputValue("Enter the name of first SimHash instance: ")
	if firstRecord == "" {
		return
	}
	if hasProbabilisticPrefix(firstRecord) {
		PrintPrefixError()
		return
	}
	firstRecord = AddPrefix(4, firstRecord)

	var secondRecord string
	secondRecord = InputValue("Enter the name of second SimHash instance: ")
	if secondRecord == "" {
		return
	}
	if hasProbabilisticPrefix(secondRecord) {
		PrintPrefixError()
		return
	}
	secondRecord = AddPrefix(4, secondRecord)

	firstRecordBytes, found := FindValue(firstRecord, lruCache, memtable)
	if found == 0 {
		fmt.Println("Couldn't find instance with name: {" + firstRecord + "}")
		return
	}
	secondRecordBytes, found := FindValue(secondRecord, lruCache, memtable)
	if found == 0 {
		fmt.Println("Couldn't find instance with name: {" + secondRecord + "}")
		return
	}
	distance := simhash.GetDistanceSimHash([16]byte(firstRecordBytes), [16]byte(secondRecordBytes))
	fmt.Println("Distance between instances {"+firstRecord+"} and {"+secondRecord+"} is {", distance, "}")
}

func SimHashOperationsMenu(wal *wal.WAL, memtable *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache, tokenBucket *tokenBucket.TokenBucket) {
	for {
		fmt.Print("--SimHash menu--\n 1. Add new instance\n 2. Compare two instances\n 0. Return to main menu\n Choose one of the options above: ")
		var typeInput int
		_, error := fmt.Scan(&typeInput)
		if error != nil {
			fmt.Println("The input is not integer! ERROR -> ", error)
			continue
		}
		if typeInput == 1 {
			SimHashAddElement(wal, memtable, lruCache, tokenBucket)
		} else if typeInput == 2 {
			SimHashCompareDistance(memtable, lruCache)
		} else if typeInput == 0 {
			fmt.Println("Exiting SimHash menu...")
			return
		} else {
			fmt.Println("Your input is invalid!")
		}
	}
}

func LoadProbabilisticConsole(wal *wal.WAL, memtable *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache, tokenBucket *tokenBucket.TokenBucket) {
	for {
		fmt.Print("--Probabilistic menu--\n 1. BloomFilter\n 2. CountMinSketch\n 3. HyperLogLog\n 4. SimHash\n 0. Return to main menu\n Choose one of the options above: ")
		var typeInput int
		_, error := fmt.Scan(&typeInput)
		if error != nil {
			fmt.Println("The input is not integer! ERROR -> ", error)
			continue
		}
		if typeInput > 0 && typeInput < 4 {
			OperationsMenu(typeInput, wal, memtable, lruCache, tokenBucket)
		} else if typeInput == 4 {
			SimHashOperationsMenu(wal, memtable, lruCache, tokenBucket)
		} else if typeInput == 0 {
			fmt.Println("Exiting probabilistic menu...")
			return
		} else {
			fmt.Println("Your input is invalid!")
		}
	}
}
