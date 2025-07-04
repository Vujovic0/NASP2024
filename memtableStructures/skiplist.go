package memtableStructures

import (
	"fmt"
	"math/rand"
	"strings"
)

type Node struct {
	value *Element
	next  []*Node // pokazivac na sledece cvorove na svakom nivou
}

type SkipList struct {
	head        *Node
	maxHeight   int
	height      int // trenutna visina liste
	lastElement *Element
}

func (s *SkipList) roll() int {
	level := 0
	// Generisanje nasumicnih visina cvorova (0 ili 1)
	for rand.Int31n(2) == 1 {
		level++
		if level >= s.maxHeight {
			return level
		}
	}
	return level
}

func newSkipList(maxHeight int) *SkipList {
	head := &Node{next: make([]*Node, maxHeight+1)}
	return &SkipList{head: head, maxHeight: maxHeight, height: 0}
}

// Funkcija za umetanje u Skip Listu
func (s *SkipList) insert(key string, value []byte, timestamp int64, tombstone bool) {
	level := s.roll()

	if level > s.height {
		s.height = level
	}

	newNode := &Node{value: &Element{Key: key, Value: value, Timestamp: timestamp, Tombstone: tombstone}, next: make([]*Node, level+1)}

	current := s.head

	update := make([]*Node, s.maxHeight+1)
	for i := s.height; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].value.Key < key {
			current = current.next[i]
		}
		update[i] = current
	}

	// Provera da li vrednost vec postoji
	if current.next[0] != nil && current.next[0].value.Key == key {
		// Ako vrednost vec postoji necemo umetati duplikat
		return
	}

	for i := 0; i <= level; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	if s.lastElement == nil || newNode.value.Timestamp > s.lastElement.Timestamp {
		s.lastElement = &Element{Key: key, Value: value, Timestamp: timestamp, Tombstone: tombstone}
	}
}

// Funkcija za logicko brisanje u Skip Listi
func (s *SkipList) delete(key string) {
	current := s.head

	// Prolazak kroz sve nivoe da bismo stigli do cvora sa datim kljucem
	for i := s.height; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].value.Key < key {
			current = current.next[i]
		}
	}

	current = current.next[0] // Na najnizem nivou

	// Ako nadjemo cvor sa trazenim kljucem, postavljamo tombstone na true
	if current != nil && current.value.Key == key {
		current.value.Tombstone = true
		fmt.Printf("Logicki obrisan kljuc: %s\n", key)
	} else {
		fmt.Printf("Kljuc %s nije pronadjen za brisanje.\n", key)
	}

	// Ako je obrisan poslednji element, pronadji novi poslednji element
	if s.lastElement != nil && s.lastElement.Key == key {
		s.lastElement = s.findNewLastElement()
	}
}

func (s *SkipList) findNewLastElement() *Element {
	current := s.head
	for current.next[0] != nil {
		current = current.next[0]
	}

	// Iteriranje unazad
	for current != s.head && current.value.Tombstone {
		current = s.findPrevious(current)
	}

	if current == s.head {
		return nil // Nije pronadjen validan element
	}

	return current.value
}

func (s *SkipList) findPrevious(node *Node) *Node {
	current := s.head
	for i := s.height; i >= 0; i-- {
		for current.next[i] != nil && current.next[i] != node {
			current = current.next[i]
		}
	}
	return current
}

// Pretraga koja ignorise logicki obrisane cvorove
func (s *SkipList) search(key string) (*Element, bool) {
	current := s.head

	// Pretraga od najviseg nivoa ka najnizem
	for i := s.height; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].value.Key < key {
			current = current.next[i]
		}
	}

	current = current.next[0] // Na najnizem nivou

	// Ako nadjemo cvor sa trazenim kljucem i nije logicki obrisan, vracamo ga
	if current != nil && current.value.Key == key && !current.value.Tombstone {
		return current.value, true
	}
	return nil, false
}

// func (s *SkipList) printAll() {
// 	current := s.head.next[0] // Pocinjemo od najnizeg nivoa
// 	for current != nil {
// 		if !current.value.Tombstone { // Preskacemo logicki obrisane elemente
// 			fmt.Printf("Key: %s, Value: %s, Timestamp: %d\n", current.value.Key, current.value.Value, current.value.Timestamp)
// 		}
// 		current = current.next[0]
// 	}
// }

func (s *SkipList) getAllElements() []*Element {
	var elements []*Element
	current := s.head.next[0] // Pocinjemo od najnizeg nivoa
	for current != nil {
		if !current.value.Tombstone { // Preskacemo logicki obrisane elemente
			elements = append(elements, current.value)
		}
		current = current.next[0]
	}
	return elements
}

func (sl *SkipList) update(element Element) {
	existingElement, found := sl.search(element.Key)
	if found {
		// Azuriramo vrednost ako kljuc vec postoji
		existingElement.Value = element.Value
		existingElement.Timestamp = element.Timestamp
		existingElement.Tombstone = element.Tombstone
	} else {
		// Umecemo novi element ako kljuc ne postoji
		sl.insert(element.Key, element.Value, element.Timestamp, element.Tombstone)
	}
}

func (sl *SkipList) LastElement() *Element {
	return sl.lastElement
}

func (sl *SkipList) searchByPrefix(prefix string) []*Element {
	var results []*Element
	current := sl.head

	// Trazimo prvi cvor koji moze da sadrzi prefiks
	for level := sl.height; level >= 0; level-- {
		for current.next[level] != nil && current.next[level].value.Key < prefix {
			current = current.next[level]
		}
	}
	current = current.next[0]

	// Prolazimo kroz sve cvorove koji pocinju sa prefixom
	for current != nil && strings.HasPrefix(current.value.Key, prefix) {
		if !current.value.Tombstone {
			results = append(results, current.value)
		}
		current = current.next[0]
	}

	return results
}

func (sl *SkipList) searchByRange(startKey, endKey string) []*Element {
	var results []*Element
	current := sl.head

	// Pronalazimo prvi cvor >= startKey
	for level := sl.height; level >= 0; level-- {
		for current.next[level] != nil && current.next[level].value.Key < startKey {
			current = current.next[level]
		}
	}
	current = current.next[0]

	// Prolazimo kroz sve cvorove u range-u [startKey, endKey]
	for current != nil && current.value.Key <= endKey {
		if current.value.Key >= startKey && !current.value.Tombstone {
			results = append(results, current.value)
		}
		current = current.next[0]
	}

	return results
}
