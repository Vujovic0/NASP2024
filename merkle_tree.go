package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/Vujovic0/NASP2024/config"
)

type Element struct {
	Key       string
	Value     []byte
	Timestamp int64
	Tombstone bool
}

type MerkleTree struct {
	Leaves     []string   // Hash-ovi listova (Element objekata)
	TreeLevels [][]string // Nivoi stabla (hijerarhija hash-ova)
}

// Funkcija za serijalizaciju Element objekta u byte array
func elementToBytes(element *Element) []byte {
	var buffer bytes.Buffer
	tmp := make([]byte, binary.MaxVarintLen64)

	// 1. Duzina kljuca
	n := binary.PutUvarint(tmp, uint64(len(element.Key)))
	buffer.Write(tmp[:n])

	// 2. Kljuc
	buffer.WriteString(element.Key)

	// 3. Timestamp
	n = binary.PutVarint(tmp, element.Timestamp)
	buffer.Write(tmp[:n])

	// 4. Tombstone
	if element.Tombstone {
		buffer.WriteByte(1)
	} else {
		buffer.WriteByte(0)
	}

	// 5. Ako element nije obrisan, upisujemo duzinu vrednosti i vrednost
	if !element.Tombstone {
		n = binary.PutUvarint(tmp, uint64(len(element.Value)))
		buffer.Write(tmp[:n])
		buffer.Write(element.Value)
	}

	return buffer.Bytes()
}

// Funkcija za heshiranje Element objekta
func hashElement(element *Element) string {
	data := elementToBytes(element)
	h := crc32.ChecksumIEEE(data)
	return fmt.Sprintf("%08x", h)
}

// Funkcija za heshiranje ulaznog stringa
func hashData(data string) string {
	h := crc32.ChecksumIEEE([]byte(data))
	return fmt.Sprintf("%08x", h)
}

// Inicijalizacija Merkle stabla sa datim Element objektima
func NewMerkleTree(elements []*Element) *MerkleTree {
	leaves := make([]string, len(elements))
	for i, element := range elements {
		leaves[i] = hashElement(element)
	}
	tree := &MerkleTree{
		Leaves: leaves,
	}

	tree.buildTree()
	return tree
}

// Novi konstruktor koji pravi MerkleTree direktno iz CRC32 hash stringova
func NewMerkleTreeFromHashes(hashes []string) *MerkleTree {
	tree := &MerkleTree{
		Leaves: hashes,
	}
	tree.buildTree()
	return tree
}

func (mt *MerkleTree) buildTree() {
	emptyElementHash := hashData("") // Hash za prazan element, tj za element koji ne postoji
	currentLevel := mt.Leaves
	mt.TreeLevels = append(mt.TreeLevels, currentLevel) // Prvi nivo su listovi

	for len(currentLevel) > 1 {
		nextLevel := []string{}

		// Spajamo cvorove u parovima
		for i := 0; i < len(currentLevel); i += 2 {
			if i+1 < len(currentLevel) {
				combined := currentLevel[i] + currentLevel[i+1]
				nextLevel = append(nextLevel, hashData(combined))
			} else {
				combined := currentLevel[i] + emptyElementHash
				nextLevel = append(nextLevel, hashData(combined))
			}
		}
		mt.TreeLevels = append(mt.TreeLevels, nextLevel) // Dodajemo novi nivo u stablo
		currentLevel = nextLevel                         // Prelazimo na sledeci nivo
	}

}

func (mt *MerkleTree) getRoot() string {
	if len(mt.TreeLevels) == 0 {
		return "" // Ako stablo nema nivoe, vracamo prazan string
	}
	lastLevel := mt.TreeLevels[len(mt.TreeLevels)-1]
	if len(lastLevel) == 0 {
		return ""
	}
	return lastLevel[0]

}

func serializeMerkleTree(mt *MerkleTree) ([]byte, error) {
	var buf bytes.Buffer

	// Serijalizujemo broj nivoa
	levelCount := uint32(len(mt.TreeLevels))
	err := binary.Write(&buf, binary.LittleEndian, levelCount)
	if err != nil {
		return nil, err
	}

	// Serijalizujemo svaki nivo
	for _, level := range mt.TreeLevels {
		// Broj hash-ova u nivou
		hashCount := uint32(len(level))
		err = binary.Write(&buf, binary.LittleEndian, hashCount)
		if err != nil {
			return nil, err
		}

		for _, hash := range level {
			if config.VariableEncoding {
				// Varijabilna duzina hash-a
				hashLen := uint32(len(hash))
				err = binary.Write(&buf, binary.LittleEndian, hashLen)
				if err != nil {
					return nil, err
				}
				_, err = buf.WriteString(hash)
				if err != nil {
					return nil, err
				}
			} else {
				// Fiksna duzina hash-a (8 bajtova za CRC32)
				// Hash je string, ali mora biti tacno 8 karaktera
				if len(hash) != 8 {
					return nil, fmt.Errorf("hash length is not 8: %s", hash)
				}
				_, err = buf.WriteString(hash)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return buf.Bytes(), nil
}

func deserializeMerkleTree(data []byte) (*MerkleTree, error) {
	buf := bytes.NewReader(data)
	var mt MerkleTree

	// Citamo broj nivoa
	var levelCount uint32
	err := binary.Read(buf, binary.LittleEndian, &levelCount)
	if err != nil {
		return nil, fmt.Errorf("failed to read level count: %w", err)
	}

	mt.TreeLevels = make([][]string, levelCount)

	for i := uint32(0); i < levelCount; i++ {
		var hashCount uint32
		err = binary.Read(buf, binary.LittleEndian, &hashCount)
		if err != nil {
			return nil, fmt.Errorf("failed to read hash count for level %d: %w", i, err)
		}

		level := make([]string, hashCount)

		for j := uint32(0); j < hashCount; j++ {
			if config.VariableEncoding {
				// Varijabilna duzina hash-a
				var hashLen uint32
				err = binary.Read(buf, binary.LittleEndian, &hashLen)
				if err != nil {
					return nil, fmt.Errorf("failed to read hash length: %w", err)
				}
				hashBytes := make([]byte, hashLen)
				bytesRead, err := buf.Read(hashBytes)
				if err != nil {
					return nil, fmt.Errorf("failed to read hash: %w", err)
				}
				if bytesRead != int(hashLen) {
					return nil, fmt.Errorf("expected to read %d bytes, got %d", hashLen, bytesRead)
				}
				level[j] = string(hashBytes)
			} else {
				// Fiksna duzina hash-a (8 bajtova)
				hashBytes := make([]byte, 8)
				bytesRead, err := buf.Read(hashBytes)
				if err != nil {
					return nil, fmt.Errorf("failed to read fixed hash: %w", err)
				}
				if bytesRead != 8 {
					return nil, fmt.Errorf("expected to read 8 bytes, got %d", bytesRead)
				}
				level[j] = string(hashBytes)
			}
		}
		mt.TreeLevels[i] = level
	}

	if len(mt.TreeLevels) > 0 {
		mt.Leaves = mt.TreeLevels[0]
	}

	return &mt, nil
}

func testSerialization() {
	testElements := []*Element{
		{Key: "user1", Value: []byte("data1"), Timestamp: 1234567890, Tombstone: false},
		{Key: "user2", Value: []byte("data2"), Timestamp: 1234567891, Tombstone: false},
	}

	// Kreiramo originalno stablo
	original := NewMerkleTree(testElements)

	// Serijalizujumo
	serialized, err := serializeMerkleTree(original)
	if err != nil {
		fmt.Printf("Serialization error: %v\n", err)
		return
	}

	// Deserijalizujemo
	deserialized, err := deserializeMerkleTree(serialized)
	if err != nil {
		fmt.Printf("Deserialization error: %v\n", err)
		return
	}

	// Uporedjujemo root hash-ove
	fmt.Printf("Original Root:     %s\n", original.getRoot())
	fmt.Printf("Deserialized Root: %s\n", deserialized.getRoot())
	fmt.Printf("Roots match: %t\n", original.getRoot() == deserialized.getRoot())
}

// Struktura za rezultat validacije
type ValidationResult struct {
	IsValid         bool
	CorruptedLevels []int // Nivoi gde su detektovane greske
	ErrorMessage    string
}

// Validacija Merkle stabla cvor po cvor
func ValidateSSTableNodeByNode(currentTree *MerkleTree, originalElements []*Element) *ValidationResult {
	result := &ValidationResult{
		IsValid:         true,
		CorruptedLevels: []int{},
		ErrorMessage:    "",
	}

	// Kreiramo ocekivano stablo iz originalnih elemenata
	expectedTree := NewMerkleTree(originalElements)

	// 1. Prvo poredimo root hash-ove
	if currentTree.getRoot() == expectedTree.getRoot() {
		result.ErrorMessage = "SSTable je validna - nema izmena"
		return result
	}

	// 2. Root se razlikuje - ima izmena, trazimo gde cvor po cvor
	result.IsValid = false
	result.ErrorMessage = "Detektovane su izmene u SSTable"

	// 3. Cvor po cvor validacija - pocinjemo od root-a
	validateNodeRecursive(currentTree, expectedTree,
		len(currentTree.TreeLevels)-1, 0, result)

	return result
}

// Rekurzivna funkcija za validaciju cvor po cvor
func validateNodeRecursive(currentTree, expectedTree *MerkleTree,
	levelIndex, nodeIndex int, result *ValidationResult) {

	// Proveravamo da li smo stigli do listova
	if levelIndex < 0 || levelIndex >= len(currentTree.TreeLevels) ||
		levelIndex >= len(expectedTree.TreeLevels) {
		return
	}

	currentLevel := currentTree.TreeLevels[levelIndex]
	expectedLevel := expectedTree.TreeLevels[levelIndex]

	// Proveravamo da li postoji cvor na ovoj poziciji
	if nodeIndex >= len(currentLevel) || nodeIndex >= len(expectedLevel) {
		return
	}

	// Poredimo trenutni cvor
	if currentLevel[nodeIndex] != expectedLevel[nodeIndex] {
		// Ovaj cvor je razlicit - oznacavamo nivo kao pokvaren
		levelAlreadyMarked := false
		for _, corruptedLevel := range result.CorruptedLevels {
			if corruptedLevel == levelIndex {
				levelAlreadyMarked = true
				break
			}
		}
		if !levelAlreadyMarked {
			result.CorruptedLevels = append(result.CorruptedLevels, levelIndex)
			result.ErrorMessage += fmt.Sprintf(" Nivo %d: cvor %d se razlikuje.", levelIndex, nodeIndex)
		}

		// Idemo na decu ovog cvora (levo i desno dete)
		if levelIndex > 0 { // Ako nismo na listovima
			leftChildIndex := nodeIndex * 2
			rightChildIndex := nodeIndex*2 + 1

			// Validiramo levo dete
			validateNodeRecursive(currentTree, expectedTree,
				levelIndex-1, leftChildIndex, result)

			// Validiramo desno dete
			validateNodeRecursive(currentTree, expectedTree,
				levelIndex-1, rightChildIndex, result)
		}
	}
	// Ako su hash-ovi identicni, ne treba ici dublje u to podstablo
}

// Simulira korisnicku validaciju SSTable
func UserValidateSSTable() {
	fmt.Println("\n VALIDACIJA SSTABLE ")

	// Simuliramo originalne podatke iz SSTable
	originalSSTableElements := []*Element{
		{Key: "user1", Value: []byte("data1"), Timestamp: 1234567890, Tombstone: false},
		{Key: "user2", Value: []byte("data2"), Timestamp: 1234567891, Tombstone: false},
		{Key: "user3", Value: []byte("data3"), Timestamp: 1234567892, Tombstone: false},
	}

	// Kreiramo trenutno stablo
	currentTree := NewMerkleTree(originalSSTableElements)
	fmt.Printf("Trenutni root: %s\n", currentTree.getRoot())

	// Test 1: Validacija neizmenjene SSTable
	fmt.Println("\n1. Validacija originalne SSTable:")
	result := ValidateSSTableNodeByNode(currentTree, originalSSTableElements)
	fmt.Printf("   Status: %t\n", result.IsValid)
	fmt.Printf("   Poruka: %s\n", result.ErrorMessage)

	// Test 2: Simuliramo izmenu podataka (kao da je SSTable ostecena)
	fmt.Println("\n2. Simulacija izmene u SSTable:")
	corruptedTree := NewMerkleTree(originalSSTableElements)
	if len(corruptedTree.TreeLevels) > 0 && len(corruptedTree.TreeLevels[0]) > 0 {
		corruptedTree.TreeLevels[0][0] = "CORRUPTED_HASH"
	}
	result = ValidateSSTableNodeByNode(corruptedTree, originalSSTableElements)
	fmt.Printf("   Status: %t\n", result.IsValid)
	fmt.Printf("   Pokvareni nivoi: %v\n", result.CorruptedLevels)
	fmt.Printf("   Poruka: %s\n", result.ErrorMessage)

	// Test 3: Izmena stvarnih podataka
	fmt.Println("\n3. Izmena podataka u SSTable:")
	modifiedElements := []*Element{
		{Key: "user1", Value: []byte("data7"), Timestamp: 1234567890, Tombstone: false},
		{Key: "user2", Value: []byte("data2"), Timestamp: 1234567891, Tombstone: false},
		{Key: "user3", Value: []byte("data3"), Timestamp: 1234567892, Tombstone: false},
	}
	modifiedTree := NewMerkleTree(modifiedElements)
	result = ValidateSSTableNodeByNode(modifiedTree, originalSSTableElements)
	fmt.Printf("   Status: %t\n", result.IsValid)
	fmt.Printf("   Pokvareni nivoi: %v\n", result.CorruptedLevels)
	fmt.Printf("   Poruka: %s\n", result.ErrorMessage)
}

// func main() {
// 	// Test podaci sa Element objektima
// 	testElements := []*Element{
// 		{Key: "user1", Value: []byte("data1"), Timestamp: 1234567890, Tombstone: false},
// 		{Key: "user2", Value: []byte("data2"), Timestamp: 1234567891, Tombstone: false},
// 		{Key: "user3", Value: []byte(""), Timestamp: 1234567892, Tombstone: true},
// 		{Key: "user4", Value: []byte("data4"), Timestamp: 1234567893, Tombstone: false},
// 	}

// 	merkleTree := NewMerkleTree(testElements)

// 	fmt.Println("Merkle Tree Levels:")
// 	for i, level := range merkleTree.TreeLevels {
// 		fmt.Printf("Level %d: %v\n", i, level)
// 	}

// 	fmt.Println("Merkle Root:", merkleTree.getRoot())

// 	testSerialization()

// 	// Korisnicka validacija SSTable
// 	UserValidateSSTable()
// }
