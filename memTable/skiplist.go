package main

import (
	"fmt"
	"math/rand"
)

type Node struct {
	value *Element
	next  []*Node // pokazivac na sledece cvorove na svakom nivou
}

type SkipList struct {
	head      *Node
	maxHeight int
	height    int // trenutna visina liste
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
func (s *SkipList) insert(key, value string, timestamp int64, tombstone bool) {
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
