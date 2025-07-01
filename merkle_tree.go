package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

type Element struct {
	Key       string
	Value     string
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
		buffer.WriteString(element.Value)
	}

	return buffer.Bytes()
}

// Funkcija za heshiranje Element objekta
func hashElement(element *Element) string {
	data := elementToBytes(element)
	h := sha256.New()
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

// Funkcija za heshiranje ulaznog stringa
func hashData(data string) string {
	h := sha256.New()
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
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

		// Svaki hash u nivou
		for _, hash := range level {
			// Duzina hash-a
			hashLen := uint32(len(hash))
			err = binary.Write(&buf, binary.LittleEndian, hashLen)
			if err != nil {
				return nil, err
			}

			// Hash string
			_, err = buf.WriteString(hash)
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

	mt.TreeLevels = make([][]string, levelCount)

	// Citamo svaki nivo
	for i := uint32(0); i < levelCount; i++ {
		// Broj hash-ova u nivou
		var hashCount uint32
		err = binary.Read(buf, binary.LittleEndian, &hashCount)
		if err != nil {
			return nil, fmt.Errorf("failed to read hash count for level %d: %w", i, err)
		}

		level := make([]string, hashCount)

		// Citamo svaki hash
		for j := uint32(0); j < hashCount; j++ {
			// Duzina hash-a
			var hashLen uint32
			err = binary.Read(buf, binary.LittleEndian, &hashLen)
			if err != nil {
				return nil, fmt.Errorf("failed to read hash length: %w", err)
			}

			// Hash string
			hashBytes := make([]byte, hashLen)
			bytesRead, err := buf.Read(hashBytes)
			if err != nil {
				return nil, fmt.Errorf("failed to read hash: %w", err)
			}
			if bytesRead != int(hashLen) {
				return nil, fmt.Errorf("expected to read %d bytes, got %d", hashLen, bytesRead)
			}

			level[j] = string(hashBytes)
		}

		mt.TreeLevels[i] = level
	}

	// Postavljamo Leaves (prvi nivo)
	if len(mt.TreeLevels) > 0 {
		mt.Leaves = mt.TreeLevels[0]
	}

	return &mt, nil
}

// Dodajte u main funkciju za testiranje:
func testSerialization() {
	testElements := []*Element{
		{Key: "user1", Value: "data1", Timestamp: 1234567890, Tombstone: false},
		{Key: "user2", Value: "data2", Timestamp: 1234567891, Tombstone: false},
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

func main() {
	// Test podaci sa Element objektima
	testElements := []*Element{
		{Key: "user1", Value: "data1", Timestamp: 1234567890, Tombstone: false},
		{Key: "user2", Value: "data2", Timestamp: 1234567891, Tombstone: false},
		{Key: "user3", Value: "", Timestamp: 1234567892, Tombstone: true},
		{Key: "user4", Value: "data4", Timestamp: 1234567893, Tombstone: false},
	}

	merkleTree := NewMerkleTree(testElements)

	fmt.Println("Merkle Tree Levels:")
	for i, level := range merkleTree.TreeLevels {
		fmt.Printf("Level %d: %v\n", i, level)
	}

	fmt.Println("Merkle Root:", merkleTree.getRoot())

	// testSerialization()
}
