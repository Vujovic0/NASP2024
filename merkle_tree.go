package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
)

type MerkleTree struct {
	Leaves     []string   // Listovi stabla (pocetni podaci)
	TreeLevels [][]string // Nivoi stabla (hijerarhija)
}

// Funkcija za heshiranje ulaznog stringa
func hashData(data string) string {
	h := sha256.New()
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
}

// Inicijalizacija Merkle stabla sa datim listovima
func NewMerkleTree(data []string) *MerkleTree {
	leaves := make([]string, len(data))
	for i, d := range data {
		leaves[i] = hashData(d)
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
				nextLevel = append(nextLevel, combined)
			}
		}
		mt.TreeLevels = append(mt.TreeLevels, nextLevel) // Dodajemo novi nivo u stablo
		currentLevel = nextLevel                         // Prelazimo na sledeci nivo
	}

}

func (mt *MerkleTree) getRoot() string {
	if len(mt.TreeLevels) == 0 {
		return " "
	}
	lastLevel := mt.TreeLevels[len(mt.TreeLevels)-1]
	if len(lastLevel) == 0 {
		return ""
	}
	return lastLevel[0]

}
func serializeMerkleTree(mt *MerkleTree) ([]byte, error) {
	var buf bytes.Buffer

	// Serijalizuja svakog nivao krenuvsi od listova
	for _, level := range mt.TreeLevels {
		// Serijalizacija niza stringova u nivou
		for _, item := range level {
			buf.Write([]byte(item))
			buf.Write([]byte{0}) // Dodajemo separator za razdvajanje elemenata u nivou
		}
		buf.Write([]byte{1}) // Dodajemo separator za razdvajanje nivoa
	}

	return buf.Bytes(), nil
}

func deserializeMerkleTree(data []byte) (MerkleTree, error) {
	var mt MerkleTree
	var currentLevel []string
	buf := bytes.NewReader(data)
	var byteValue byte

	// Citanje podataka level po level
	for {
		byteValue, _ = buf.ReadByte()
		if byteValue == 1 { // Separator izmedju nivoa
			mt.TreeLevels = append(mt.TreeLevels, currentLevel)
			currentLevel = []string{}
		} else if byteValue == 0 {
			continue

		} else {
			// Citanje hash-a
			var hash string
			for byteValue != 0 && byteValue != 1 {
				hash += string(byteValue)
				byteValue, _ = buf.ReadByte()
			}
			currentLevel = append(currentLevel, hash)
		}

		// Ako je kraj
		if byteValue == 0 {
			break
		}
	}
	// Dodavanje poslednjeg nivoa
	if len(currentLevel) > 0 {
		mt.TreeLevels = append(mt.TreeLevels, currentLevel)
	}
	return mt, nil
}

// func main() {
// 	data := []string{"leaf1", "leaf2", "leaf3", "leaf4"}
// 	merkleTree := NewMerkleTree(data)

// 	fmt.Println("Merkle Tree Levels:")
// 	for i, level := range merkleTree.TreeLevels {
// 		fmt.Printf("Level %d: %v\n", i, level)
// 	}

// 	fmt.Println("Merkle Root:", merkleTree.getRoot())
// }
