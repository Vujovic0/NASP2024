package memtableStructures

import (
	"fmt"
	"sort"
	"strings"
)

type HashMapNode struct {
	Key   string
	Value *Element
	Next  *HashMapNode
}

type HashMap struct {
	buckets     []*HashMapNode
	size        int
	lastElement *Element
}

func newHashMap(size int) *HashMap {
	return &HashMap{
		buckets: make([]*HashMapNode, size),
		size:    size,
	}
}

func hashFunction(key string, size int) uint {
	return uint(len(key) % size)
}

func (hm *HashMap) insert(element Element) {
	existingElement, found := hm.search(element.Key)
	if found {
		// Update postojeci element
		existingElement.Value = element.Value
		existingElement.Timestamp = element.Timestamp
		existingElement.Tombstone = element.Tombstone
		return
	}

	// Dodajemo novi element ako ne postoji
	// Racunamo index gde cemo smestiti nas novi kljuc-vrednost par
	index := hashFunction(element.Key, hm.size)

	newNode := &HashMapNode{
		Key:   element.Key,
		Value: &element,
		Next:  hm.buckets[index],
	}
	hm.buckets[index] = newNode

	// Azuriranje poslednjeg elementa
	if hm.lastElement == nil || element.Timestamp > hm.lastElement.Timestamp {
		hm.lastElement = &element
	}
}

func (hm *HashMap) search(key string) (*Element, bool) {
	// Racunamo index pomocu hash funkcije da bi znali gde trebamo traziti
	index := hashFunction(key, hm.size)
	current := hm.buckets[index]
	for current != nil {
		if current.Key == key && !current.Value.Tombstone {
			return current.Value, true
		}
		current = current.Next
	}

	return nil, false
}

func (hm *HashMap) delete(key string) {
	index := hashFunction(key, hm.size)
	current := hm.buckets[index]

	for current != nil {
		if current.Key == key {
			current.Value.Tombstone = true
			fmt.Printf("Logicki obrisan kljuc: %s\n", key)

			// Ako je obrisan poslednji element, pronadji novi poslednji element
			if hm.lastElement != nil && hm.lastElement.Key == key {
				hm.lastElement = hm.findNewLastElement()
			}

			return
		}
		current = current.Next
	}
	fmt.Printf("Kljuc %s nije pronadjen za brisanje.\n", key)
}

func (hm *HashMap) findNewLastElement() *Element {
	var lastElement *Element

	for _, bucket := range hm.buckets {
		current := bucket
		for current != nil {
			if !current.Value.Tombstone && (lastElement == nil || current.Value.Timestamp > lastElement.Timestamp) {
				lastElement = current.Value
			}
			current = current.Next
		}
	}

	return lastElement
}

func (hm *HashMap) update(element Element) {
	existingElement, found := hm.search(element.Key)

	if found {
		// Azuriramo vrednost ako kljuc vec postoji
		existingElement.Value = element.Value
		existingElement.Timestamp = element.Timestamp
		existingElement.Tombstone = element.Tombstone

	} else {
		// Umecemo novi element ako kljuc ne postoji
		hm.insert(element)
	}
}

func (hm *HashMap) getAllElements() []*Element {
	var elements []*Element
	for _, node := range hm.buckets {
		for node != nil {
			if node.Value != nil && !node.Value.Tombstone {
				elements = append(elements, node.Value)
			}
			node = node.Next
		}
	}
	return elements
}

func (hm *HashMap) LastElement() *Element {
	return hm.lastElement
}

func (hm *HashMap) searchByPrefix(prefix string) []*Element {
	var results []*Element

	// Prolazimo kroz sve bucket-e
	for i := 0; i < len(hm.buckets); i++ {
		current := hm.buckets[i]
		for current != nil {
			if strings.HasPrefix(current.Value.Key, prefix) && !current.Value.Tombstone {
				results = append(results, current.Value)
			}
			current = current.Next
		}
	}

	// Sortiraj rezultate po kljucu
	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})

	return results
}

func (hm *HashMap) searchByRange(startKey, endKey string) []*Element {
	var results []*Element

	// Prolazimo kroz sve bucket-e
	for i := 0; i < len(hm.buckets); i++ {
		current := hm.buckets[i]
		for current != nil {
			if current.Value.Key >= startKey && current.Value.Key <= endKey && !current.Value.Tombstone {
				results = append(results, current.Value)
			}
			current = current.Next
		}
	}

	// Sortiramo rezultate po kljucu
	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})

	return results
}

// func main() {
// 	// Create a new hashmap with size 10
// 	myHashMap := newHashMap(10)

// 	// // Insert key-value pairs
// 	// myHashMap.Insert("john", "doe")
// 	// myHashMap.Insert("foo", "bar")

// 	// Get and print values
// 	value := myHashMap.Get("john")
// 	fmt.Println("Value for key john:", value)

// 	// Delete a key
// 	myHashMap.Delete("foo")
// 	/* If we try to get the value for key "foo" we will get an empty string. (You can return a
// 	   proper error or a flag in your get method) */
// }
