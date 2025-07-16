package console

import (
	"fmt"

	"github.com/Vujovic0/NASP2024/config"
	"github.com/Vujovic0/NASP2024/memtableStructures"
	"github.com/Vujovic0/NASP2024/probabilisticDataStructures"
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

func CreateNewInstance(typeInput int, wal *wal.WAL, memtable *memtableStructures.MemTableManager) {

	fmt.Println("Enter the name of new instance: ")
	var instanceName string
	_, error := fmt.Scan(&instanceName)
	if error != nil {
		fmt.Println("Error while loading input name...")
		return
	}
	if hasProbabilisticPrefix(instanceName) {
		fmt.Println("You entered a key with reserved prefix!")
		fmt.Println("bf_ - BloomFilter")
		fmt.Println("cms_ - CountMinSketch")
		fmt.Println("hpp_ - HyperLogLog")
		fmt.Println("sm_ - SimHash")
		return
	}
	instanceName = AddPrefix(typeInput, instanceName)
	switch typeInput {
	case 1:
		elementsNum, falsePositive := BloomFilterParametersInput()
		bf := probabilisticDataStructures.MakeBloomFilter(elementsNum, falsePositive)
		bfBytes := probabilisticDataStructures.SerializeToBytes(bf)
		offset, err := wal.WriteLogEntry(instanceName, bfBytes, false)
		if err != nil {
			fmt.Println("Error happend while writing WAL! Returning...")
			return
		}
		memtable.Insert(instanceName, bfBytes, false, wal.CurrentFile.Name(), wal.CurrentBlock, offset)
	}
}

func DeleteExistingInstance(typeInput int) {

}

func AddElement(typeInput int) {

}

func OperationsMenu(typeInput int, wal *wal.WAL, memtable *memtableStructures.MemTableManager) {
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
	fmt.Print("--" + typeName + " operations menu--\n 1. NEW INSTANCE\n 2. DELETE EXISTING INSTANCE\n 3. ADD NEW ELEMENT\n 4. " + specificOperation + "\n 0. EXIT\n Choose one of the options above: ")
	var operationInput int
	_, error := fmt.Scan(&operationInput)
	for {
		if error != nil {
			fmt.Println("The input is not integer! ERROR -> ", error)
			continue
		}
		switch operationInput {
		case 1:
			CreateNewInstance(typeInput, wal, memtable)
		case 2:
			DeleteExistingInstance(typeInput)
		case 3:
			AddElement(typeInput)
		case 4:
			// OPERACIJA SPECIFIČNA TIPU
		case 0:
			fmt.Println("Returning to probabilistic menu...")
			return
		default:
			fmt.Println("Your input is invalid!")
		}
	}

}

func LoadProbabilisticConsole(wal *wal.WAL, memtable *memtableStructures.MemTableManager) {
	for {
		fmt.Print("--Probabilistic menu--\n 1. BloomFilter\n 2. CountMinSketch\n 3. HyperLogLog\n 4. SimHash\n 0. Return to main menu\n Choose one of the options above: ")
		var typeInput int
		_, error := fmt.Scan(&typeInput)
		if error != nil {
			fmt.Println("The input is not integer! ERROR -> ", error)
			continue
		}
		if typeInput > 0 && typeInput < 4 {
			OperationsMenu(typeInput, wal, memtable)
		} else if typeInput == 4 {
			//SimHashOperationsMenu()
		} else if typeInput == 0 {
			fmt.Println("Exiting probabilistic menu...")
			return
		} else {
			fmt.Println("Your input is invalid!")
		}
	}
}
