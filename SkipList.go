package main

import (
	"fmt"
	"math/rand"
)

type Node struct {
	value int
	next  []*Node //pokazivac na sledece cvorove na svakom nivou
}

type SkipList struct {
	head      *Node
	maxHeight int
	height    int // trenutna visina liste
}

func (s *SkipList) roll() int {
	level := 0
	// possible ret values from rand are 0 and 1
	// we stop when we get a 0
	for ; rand.Int31n(2) == 1; level++ {
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

// Funkcija za pretragu u Skip listi
func (s *SkipList) search(value int) *Node {
	current := s.head

	// Krecemo od najviseg nivoa
	for i := s.height; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].value < value {
			current = current.next[i]
		}
	}

	current = current.next[0]

	if current != nil && current.value == value {
		return current
	}
	return nil
}

// Funkcija za dodavanje u Skip listu
func (s *SkipList) insert(value int) {
	level := s.roll()

	if level > s.height {
		s.height = level
	}

	newNode := &Node{value: value, next: make([]*Node, level+1)}

	current := s.head

	update := make([]*Node, s.maxHeight+1)
	for i := s.height; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].value < value {
			current = current.next[i]
		}
		update[i] = current
	}

	// Provera da li vrednost vec postoji
	if current.next[0] != nil && current.next[0].value == value {
		// Ako vrednost vec postoji necemo umetati duplikat
		return
	}

	for i := 0; i <= level; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}
}

// Funkcija za brisanje u Skip listi
func (s *SkipList) delete(value int) {
	current := s.head

	// Niz za azuriranje pokazivaca
	update := make([]*Node, s.maxHeight+1)

	// Pretraga pozicija za azuriranje
	for i := s.height; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].value < value {
			current = current.next[i]
		}
		update[i] = current
	}

	// Provera da li postoji novi cvor za brisanje
	current = current.next[0]
	if current != nil && current.value == value {
		for i := 0; i <= s.height; i++ {
			if update[i].next[i] != current {
				break
			}
			update[i].next[i] = current.next[i]
		}

		// Smanjivanje visine ako je poslednji nivo sada prazan
		for s.height > 0 && s.head.next[s.height] == nil {
			s.height--
		}
	}

}

func main() {
	s := newSkipList(3)

	for i := 0; i < 10; i++ {
		s.insert(i)
	}

	fmt.Println("Pretraga elementa 1", s.search(1))
	fmt.Println("Pretraga elementa 14", s.search(14))
	fmt.Println("Pretraga elementa 5", s.search(5))
	fmt.Println("Pretraga elementa 10", s.search(10))
	s.delete(2)
	fmt.Println("Pretraga elementa 2", s.search(2))
}
