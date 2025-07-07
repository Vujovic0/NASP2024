package ssTable

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
	Leaves     []uint32   // Hash-ovi listova (Element objekata)
	TreeLevels [][]uint32 // Nivoi stabla (hijerarhija hash-ova)
}

// Funkcija za serijalizaciju Element objekta u byte array
func elementToBytes(element *Element) []byte {
	var buffer bytes.Buffer
	tmp := make([]byte, binary.MaxVarintLen64)

	// 1. Timestamp
	if config.VariableEncoding {
		n := binary.PutVarint(tmp, element.Timestamp)
		buffer.Write(tmp[:n])
	} else {
		binary.Write(&buffer, binary.LittleEndian, element.Timestamp)
	}

	// 2. Tombstone
	if element.Tombstone {
		buffer.WriteByte(1)
	} else {
		buffer.WriteByte(0)
	}

	if config.VariableEncoding {
		// 3. Duzina kljuca
		tmp := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(tmp, uint64(len(element.Key)))
		buffer.Write(tmp[:n])

		// 4. Duzina vrednosti - samo ako nije tombstone
		if !element.Tombstone {
			n = binary.PutUvarint(tmp, uint64(len(element.Value)))
			buffer.Write(tmp[:n])
		}
	} else {
		// 3. Duzina kljuca (uint64)
		keySize := uint64(len(element.Key))
		binary.Write(&buffer, binary.LittleEndian, keySize)

		// 4. Duzina vrednosti (uint64) - samo ako nije tombstone
		if !element.Tombstone {
			valueSize := uint64(len(element.Value))
			binary.Write(&buffer, binary.LittleEndian, valueSize)
		}
	}

	// 5. Kljuc
	buffer.WriteString(element.Key)

	// 6. Ako element nije obrisan, upisujemo vrednost
	if !element.Tombstone {
		buffer.Write(element.Value)
	}

	return buffer.Bytes()
}

// Funkcija za heshiranje Element objekta
func hashElement(element *Element) uint32 {
	data := elementToBytes(element)
	h := crc32.ChecksumIEEE(data)
	return h
}

// Funkcija za heshiranje ulaznog stringa
func hashData(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// Inicijalizacija Merkle stabla sa datim Element objektima
func NewMerkleTree(elements []*Element) *MerkleTree {
	leaves := make([]uint32, len(elements))
	for i, element := range elements {
		leaves[i] = hashElement(element)
	}
	tree := &MerkleTree{
		Leaves: leaves,
	}

	tree.buildTree()
	return tree
}

// Novi konstruktor koji pravi MerkleTree direktno iz CRC32 hash vrednosti
func NewMerkleTreeFromHashes(hashes []uint32) *MerkleTree {
	tree := &MerkleTree{
		Leaves: hashes,
	}
	tree.buildTree()
	return tree
}

func (mt *MerkleTree) buildTree() {
	emptyElementHash := hashData([]byte("")) // Hash za prazan element, tj za element koji ne postoji
	currentLevel := mt.Leaves
	mt.TreeLevels = append(mt.TreeLevels, currentLevel) // Prvi nivo su listovi

	for len(currentLevel) > 1 {
		nextLevel := []uint32{}

		// Spajamo cvorove u parovima
		for i := 0; i < len(currentLevel); i += 2 {
			buf := new(bytes.Buffer)
			binary.Write(buf, binary.LittleEndian, currentLevel[i])
			if i+1 < len(currentLevel) {
				binary.Write(buf, binary.LittleEndian, currentLevel[i+1])
			} else {
				binary.Write(buf, binary.LittleEndian, emptyElementHash)
			}
			nextLevel = append(nextLevel, hashData(buf.Bytes()))
		}
		mt.TreeLevels = append(mt.TreeLevels, nextLevel) // Dodajemo novi nivo u stablo
		currentLevel = nextLevel                         // Prelazimo na sledeci nivo
	}

}

func (mt *MerkleTree) GetRoot() uint32 {
	if len(mt.TreeLevels) == 0 {
		return 0 // Ako stablo nema nivoe, vracamo 0
	}
	lastLevel := mt.TreeLevels[len(mt.TreeLevels)-1]
	if len(lastLevel) == 0 {
		return 0
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
			err = binary.Write(&buf, binary.LittleEndian, hash)
			if err != nil {
				return nil, err
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

	mt.TreeLevels = make([][]uint32, levelCount)

	for i := uint32(0); i < levelCount; i++ {
		var hashCount uint32
		err = binary.Read(buf, binary.LittleEndian, &hashCount)
		if err != nil {
			return nil, fmt.Errorf("failed to read hash count for level %d: %w", i, err)
		}

		level := make([]uint32, hashCount)

		for j := uint32(0); j < hashCount; j++ {
			var hash uint32
			err = binary.Read(buf, binary.LittleEndian, &hash)
			if err != nil {
				return nil, fmt.Errorf("failed to read hash: %w", err)
			}
			level[j] = hash
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

	// Serijalizujemo
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
	fmt.Printf("Original Root:     %d\n", original.GetRoot())
	fmt.Printf("Deserialized Root: %d\n", deserialized.GetRoot())
	fmt.Printf("Roots match: %t\n", original.GetRoot() == deserialized.GetRoot())
}

// Struktura za rezultat validacije
type ValidationResult struct {
	IsValid         bool
	CorruptedHashes []uint32 // Hash vrednosti cvorova koji su razliciti
	ErrorMessage    string
}

// Validacija Merkle stabla cvor po cvor
func ValidateSSTableNodeByNode(currentTree *MerkleTree, originalElements []*Element) *ValidationResult {
	result := &ValidationResult{
		IsValid:         true,
		CorruptedHashes: []uint32{},
		ErrorMessage:    "",
	}

	// Kreiramo ocekivano stablo iz originalnih elemenata
	expectedTree := NewMerkleTree(originalElements)

	// 1. Prvo poredimo root hash-ove
	if currentTree.GetRoot() == expectedTree.GetRoot() {
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
		result.CorruptedHashes = append(result.CorruptedHashes, currentLevel[nodeIndex])
		result.ErrorMessage += fmt.Sprintf(" CRC32: %d (level %d, node %d) se razlikuje.", currentLevel[nodeIndex], levelIndex, nodeIndex)

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
	fmt.Printf("Trenutni root: %d\n", currentTree.GetRoot())

	// Test 1: Validacija neizmenjene SSTable
	fmt.Println("\n1. Validacija originalne SSTable:")
	result := ValidateSSTableNodeByNode(currentTree, originalSSTableElements)
	fmt.Printf("   Status: %t\n", result.IsValid)
	fmt.Printf("   Poruka: %s\n", result.ErrorMessage)

	// Test 2: Simuliramo izmenu podataka (kao da je SSTable ostecena)
	fmt.Println("\n2. Simulacija izmene u SSTable:")
	corruptedTree := NewMerkleTree(originalSSTableElements)
	if len(corruptedTree.TreeLevels) > 0 && len(corruptedTree.TreeLevels[0]) > 0 {
		corruptedTree.TreeLevels[0][0] = hashData([]byte("izmenjeni_hash"))
	}
	result = ValidateSSTableNodeByNode(corruptedTree, originalSSTableElements)
	fmt.Printf("   Status: %t\n", result.IsValid)
	fmt.Printf("   Pokvareni CRC32: %v\n", result.CorruptedHashes)
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
	fmt.Printf("   Pokvareni CRC32: %v\n", result.CorruptedHashes)
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
