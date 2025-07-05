package memtableStructures

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"
)

type MemTableManager struct {
	active        *MemoryTable   // read-write MemTable
	immutables    []*MemoryTable // read-only MemTables (stare, cekaju flush)
	maxImmutables int            // n-1 read-only tabela
}

func NewMemTableManager(maxImmutables int) *MemTableManager {
	return &MemTableManager{
		active:        initializeMemoryTable(),
		immutables:    []*MemoryTable{},
		maxImmutables: maxImmutables,
	}
}

func (mtm *MemTableManager) Insert(key string, value []byte) {
	mtm.active.Insert(key, value)
	// fmt.Printf("CurrentSize: %d, MaxSize: %d\n", mtm.active.CurrentSize, mtm.active.MaxSize)
	if mtm.active.CurrentSize >= int(mtm.active.MaxSize) {
		mtm.FlushActive()
	}
}

func (mtm *MemTableManager) Search(key string) (*Element, bool) {
	elem, found := mtm.active.Search(key)
	if found {
		return elem, true
	}
	for _, mt := range mtm.immutables {
		elem, found := mt.Search(key)
		if found {
			return elem, true
		}
	}
	return nil, false
}

func (mtm *MemTableManager) FlushActive() {
	fmt.Println("Aktivna memtable je puna...")

	// Prebacujemo aktivnu tabelu u read-only
	mtm.immutables = append([]*MemoryTable{mtm.active}, mtm.immutables...)

	// Sada proveravamo da li ih ima n (n-1 read-only + 1 aktivna koja je sada postala read-only)
	if len(mtm.immutables) > mtm.maxImmutables {
		fmt.Println("SVE tabele su pune! Flushujem SVE na disk...")
		mtm.FlushAll()
	}

	mtm.active = initializeMemoryTable()
}

func (mtm *MemTableManager) convertToSSTableFormat(elements []Element) []byte {
	var data []byte

	for _, elem := range elements {
		entry := make([]byte, 0)

		// CRC (4 bytes) - calculate later
		entry = append(entry, make([]byte, 4)...)

		// Timestamp (8 bytes)
		timestampBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(timestampBytes, uint64(elem.Timestamp))
		entry = append(entry, timestampBytes...)

		// Tombstone (1 byte)
		if elem.Tombstone {
			entry = append(entry, 1)
		} else {
			entry = append(entry, 0)
		}

		// Key size (8 bytes)
		keySizeBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(keySizeBytes, uint64(len(elem.Key)))
		entry = append(entry, keySizeBytes...)

		if !elem.Tombstone {
			// Value size (8 bytes) - only for non-tombstone
			valueSizeBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(valueSizeBytes, uint64(len(elem.Value)))
			entry = append(entry, valueSizeBytes...)
		}

		// Key
		entry = append(entry, []byte(elem.Key)...)

		if !elem.Tombstone {
			// Value - only for non-tombstone
			entry = append(entry, elem.Value...)
		}

		// Calculate CRC for everything except first 4 bytes
		crc := crc32.ChecksumIEEE(entry[4:])
		binary.LittleEndian.PutUint32(entry[0:4], crc)

		data = append(data, entry...)
	}

	return data
}

func (mtm *MemTableManager) FlushAll() {
	// Flushuj sve read-only tabele
	for i := len(mtm.immutables) - 1; i >= 0; i-- {
		// fmt.Printf("Flushujem read-only tabelu %d na disk...\n", i+1)
		mtm.immutables[i].Flush()
	}
	// Cistimo sve tabele iz memorije
	mtm.immutables = []*MemoryTable{}
}

// func (mtm *MemTableManager) FlushAll() {
// 	fmt.Printf("Flushujem %d read-only tabela na disk...\n", len(mtm.immutables))

// 	for i, memTable := range mtm.immutables {
// 		elements := memTable.Flush()

// 		if len(elements) > 0 {
// 			// 1. Konvertujemo u SSTable format
// 			sstableData := mtm.convertToSSTableFormat(elements)

// 			// 2. Kreiramo boundary element (poslednji kljuc)
// 			lastKey := elements[len(elements)-1].Key
// 			lastElementData := make([]byte, 8+len(lastKey))
// 			binary.LittleEndian.PutUint64(lastElementData[0:8], uint64(len(lastKey)))
// 			copy(lastElementData[8:], []byte(lastKey))

// 			// 3. Kreiramo kompaktni sstable
// 			ssTable.CreateCompactSSTable(sstableData, lastElementData, 10, 5)

// 			fmt.Printf("✅ SSTable_%d kreiran sa %d elemenata\n", i+1, len(elements))
// 		}
// 	}

// 	mtm.immutables = []*MemoryTable{}
// 	fmt.Println("🚀 Svi SSTable fajlovi kreirani!")
// }

// func main() {
// 	var n int
// 	fmt.Print("Unesi ukupan broj MemTable-ova u memoriji (n): ")
// 	fmt.Scan(&n)
// 	mtm := NewMemTableManager(n - 1) // n-1 read-only, 1 read-write

// 	mtm.Insert("key2", []byte("value2"))
// 	mtm.Insert("key3", []byte("value3"))
// 	mtm.Insert("key4", []byte("value4"))

// 	elem, found := mtm.Search("key2")
// 	if found {
// 		fmt.Printf("Found: %s -> %s\n", elem.Key, elem.Value)
// 	} else {
// 		fmt.Println("Element not found")
// 	}

// 	// ...ostalo kao i do sada, koristi mtm.active za serijalizaciju, flush itd.
// }

func (mtm *MemTableManager) SearchByPrefix(prefix string) ([]*Element, bool) {
	var allResults []*Element

	// Pretrazujemo aktivnu tabelu
	results, found := mtm.active.SearchByPrefix(prefix)
	if found {
		allResults = append(allResults, results...)
	}

	// Pretrazujemo sve read-only tabele
	for _, mt := range mtm.immutables {
		results, found := mt.SearchByPrefix(prefix)
		if found {
			allResults = append(allResults, results...)
		}
	}

	// Sortiramo konacne rezultate
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Key < allResults[j].Key
	})

	return allResults, len(allResults) > 0
}

func (mtm *MemTableManager) PrefixScan(prefix string, pageNumber int, pageSize int) ([]*Element, bool) {
	var allResults []*Element

	// Pretrazujemo aktivnu tabelu
	results, found := mtm.active.SearchByPrefix(prefix)
	if found {
		allResults = append(allResults, results...)
	}

	// Pretrazujemo sve read-only tabele
	for _, mt := range mtm.immutables {
		results, found := mt.SearchByPrefix(prefix)
		if found {
			allResults = append(allResults, results...)
		}
	}

	// Sortiramo konacne rezultate
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Key < allResults[j].Key
	})

	// Implementiramo paginaciju
	totalResults := len(allResults)
	if totalResults == 0 {
		return []*Element{}, false
	}

	// Racunamo start i end indekse za trazenu stranicu
	startIndex := (pageNumber - 1) * pageSize
	endIndex := startIndex + pageSize

	// Proveravamo da li je stranica validna
	if startIndex >= totalResults {
		return []*Element{}, false // Stranica ne postoji
	}

	// Ogranicavamo end index na ukupan broj rezultata
	if endIndex > totalResults {
		endIndex = totalResults
	}

	// Vracamo samo rezultate za trazenu stranicu
	pageResults := allResults[startIndex:endIndex]

	return pageResults, len(pageResults) > 0
}

type KeyRange struct {
	StartKey string // k1 - minimalna vrednost kljuca
	EndKey   string // k2 - maksimalna vrednost kljuca
}

func (mtm *MemTableManager) RangeScan(keyRange KeyRange, pageNumber int, pageSize int) ([]*Element, bool) {
	var allResults []*Element

	// Pretrazujemo aktivnu tabelu
	results, found := mtm.active.SearchByRange(keyRange.StartKey, keyRange.EndKey)
	if found {
		allResults = append(allResults, results...)
	}

	// Pretrazujemo sve read-only tabele
	for _, mt := range mtm.immutables {
		results, found := mt.SearchByRange(keyRange.StartKey, keyRange.EndKey)
		if found {
			allResults = append(allResults, results...)
		}
	}

	// Sortiramo konacne rezultate po kljucu
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Key < allResults[j].Key
	})

	// Implementiramo paginaciju
	totalResults := len(allResults)
	if totalResults == 0 {
		return []*Element{}, false
	}

	// Racunamo start i end indekse za trazenu stranicu
	startIndex := (pageNumber - 1) * pageSize
	endIndex := startIndex + pageSize

	// Proveravamo da li je stranica validna
	if startIndex >= totalResults {
		return []*Element{}, false // Stranica ne postoji
	}

	// Ogranicavamo end index na ukupan broj rezultata
	if endIndex > totalResults {
		endIndex = totalResults
	}

	// Vracamo samo rezultate za trazenu stranicu
	pageResults := allResults[startIndex:endIndex]

	return pageResults, len(pageResults) > 0
}

// func main() {
// 	mtm := NewMemTableManager(2)

// 	// Dodajemo test podatke
// 	mtm.Insert("apple", []byte("fruit1"))
// 	mtm.Insert("application", []byte("software"))
// 	mtm.Insert("apply", []byte("verb"))
// 	mtm.Insert("approach", []byte("method"))
// 	mtm.Insert("approve", []byte("accept"))
// 	mtm.Insert("banana", []byte("fruit2"))
// 	mtm.Insert("car", []byte("vehicle"))
// 	mtm.Insert("cat", []byte("animal"))

// 	// Test Range Scan
// 	fmt.Println("=== RANGE_SCAN(['apple', 'approve'], pageNumber, pageSize=3) ===")

// 	keyRange := KeyRange{
// 		StartKey: "apple",
// 		EndKey:   "approve",
// 	}

// 	// Stranica 1
// 	results, found := mtm.RangeScan(keyRange, 1, 3)
// 	if found {
// 		fmt.Printf("Stranica 1 (3 rezultata):\n")
// 		for _, elem := range results {
// 			fmt.Printf("  Key: %s, Value: %s\n", elem.Key, elem.Value)
// 		}
// 	}

// 	// Stranica 2
// 	results, found = mtm.RangeScan(keyRange, 2, 3)
// 	if found {
// 		fmt.Printf("Stranica 2 (preostali rezultati):\n")
// 		for _, elem := range results {
// 			fmt.Printf("  Key: %s, Value: %s\n", elem.Key, elem.Value)
// 		}
// 	} else {
// 		fmt.Println("Stranica 2: Nema vise rezultata")
// 	}
// }

type Iterator struct {
	elements []*Element
	current  int
	stopped  bool
}

func (iter *Iterator) Next() (*Element, bool) {
	if iter.stopped || iter.current >= len(iter.elements) {
		return nil, false
	}

	element := iter.elements[iter.current]
	iter.current++
	return element, true
}

func (iter *Iterator) Stop() {
	iter.stopped = true
}

func (iter *Iterator) HasNext() bool {
	return !iter.stopped && iter.current < len(iter.elements)
}

// Resetujemo iterator na pocetak
func (iter *Iterator) Reset() {
	iter.current = 0
	iter.stopped = false
}

func (mtm *MemTableManager) PrefixIterate(prefix string) *Iterator {
	var allResults []*Element

	// Pretrazujemo aktivnu tabelu
	results, found := mtm.active.SearchByPrefix(prefix)
	if found {
		allResults = append(allResults, results...)
	}

	// Pretrazujemo sve read-only tabele
	for _, mt := range mtm.immutables {
		results, found := mt.SearchByPrefix(prefix)
		if found {
			allResults = append(allResults, results...)
		}
	}

	// Sortiramo rezultate rastuce po kljucu
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Key < allResults[j].Key
	})

	return &Iterator{
		elements: allResults,
		current:  0,
		stopped:  false,
	}
}

func (mtm *MemTableManager) RangeIterate(keyRange KeyRange) *Iterator {
	var allResults []*Element

	// Pretrazujemo aktivnu tabelu
	results, found := mtm.active.SearchByRange(keyRange.StartKey, keyRange.EndKey)
	if found {
		allResults = append(allResults, results...)
	}

	// Pretrazujemo sve read-only tabele
	for _, mt := range mtm.immutables {
		results, found := mt.SearchByRange(keyRange.StartKey, keyRange.EndKey)
		if found {
			allResults = append(allResults, results...)
		}
	}

	// Sortiramo rezultate rastuce po kljucu
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Key < allResults[j].Key
	})

	return &Iterator{
		elements: allResults,
		current:  0,
		stopped:  false,
	}
}

// func main() {
// 	mtm := NewMemTableManager(2)

// 	// Dodajemo test podatke
// 	mtm.Insert("app", []byte("mobile"))
// 	mtm.Insert("apple", []byte("fruit1"))
// 	mtm.Insert("application", []byte("software"))
// 	mtm.Insert("apply", []byte("verb"))
// 	mtm.Insert("approach", []byte("method"))
// 	mtm.Insert("approve", []byte("accept"))
// 	mtm.Insert("banana", []byte("fruit2"))

// 	// Test paginacije sa pageSize = 2
// 	fmt.Println("=== PREFIX_SCAN('app', pageNumber, pageSize=2) ===")

// 	// Stranica 1
// 	results, found := mtm.PrefixScan("app", 1, 2)
// 	if found {
// 		fmt.Printf("Stranica 1 (2 rezultata):\n")
// 		for _, elem := range results {
// 			fmt.Printf("  Key: %s, Value: %s\n", elem.Key, elem.Value)
// 		}
// 	}

// 	// Stranica 2
// 	results, found = mtm.PrefixScan("app", 2, 2)
// 	if found {
// 		fmt.Printf("Stranica 2 (2 rezultata):\n")
// 		for _, elem := range results {
// 			fmt.Printf("  Key: %s, Value: %s\n", elem.Key, elem.Value)
// 		}
// 	}

// 	// Stranica 3
// 	results, found = mtm.PrefixScan("app", 3, 2)
// 	if found {
// 		fmt.Printf("Stranica 3 (preostali rezultati):\n")
// 		for _, elem := range results {
// 			fmt.Printf("  Key: %s, Value: %s\n", elem.Key, elem.Value)
// 		}
// 	}

// 	// Stranica 4 (ne postoji)
// 	results, found = mtm.PrefixScan("app", 4, 2)
// 	if !found {
// 		fmt.Println("Stranica 4: Nema vise rezultata")
// 	}
// }

// func main() {
// 	var n int = 3                    // 2 read-only + 1 aktivna
// 	mtm := NewMemTableManager(n - 1) // n-1 read-only, 1 read-write

// 	// Upisujemo 12 zapisa da popunimo sve tabele i izazovemo flush
// 	for i := 1; i <= 12; i++ {
// 		key := fmt.Sprintf("key%d", i)
// 		value := []byte(fmt.Sprintf("value%d", i))
// 		mtm.Insert(key, value)
// 	}
// }

func main() {
	mtm := NewMemTableManager(2)

	// Dodajemo test podatke
	mtm.Insert("apple", []byte("fruit1"))
	mtm.Insert("application", []byte("software"))
	mtm.Insert("apply", []byte("verb"))
	mtm.Insert("approach", []byte("method"))
	mtm.Insert("approve", []byte("accept"))
	mtm.Insert("app", []byte("mobile"))
	mtm.Insert("banana", []byte("fruit2"))

	// Test PREFIX_ITERATE
	fmt.Println("=== PREFIX_ITERATE('app') ===")
	prefixIter := mtm.PrefixIterate("app")

	for prefixIter.HasNext() {
		elem, found := prefixIter.Next()
		if found {
			fmt.Printf("Key: %s, Value: %s\n", elem.Key, elem.Value)
		}
	}
	prefixIter.Stop()

	fmt.Println("\n=== RANGE_ITERATE(['apple', 'approve']) ===")
	// Test RANGE_ITERATE
	keyRange := KeyRange{
		StartKey: "apple",
		EndKey:   "approve",
	}

	rangeIter := mtm.RangeIterate(keyRange)

	for rangeIter.HasNext() {
		elem, found := rangeIter.Next()
		if found {
			fmt.Printf("Key: %s, Value: %s\n", elem.Key, elem.Value)
		}
	}
	rangeIter.Stop()

	// Test interaktivnog rada sa iteratorom
	fmt.Println("\n=== INTERAKTIVNI ITERATOR ===")
	iter := mtm.PrefixIterate("app")

	// Uzmi prva 2 elementa
	fmt.Println("Prvi element:")
	if elem, found := iter.Next(); found {
		fmt.Printf("  Key: %s, Value: %s\n", elem.Key, elem.Value)
	}

	fmt.Println("Drugi element:")
	if elem, found := iter.Next(); found {
		fmt.Printf("  Key: %s, Value: %s\n", elem.Key, elem.Value)
	}

	// Prekini rad sa iteratorom
	iter.Stop()

	// Pokusaj da uzmes sledeci
	fmt.Println("Pokusaj posle stop():")
	if elem, found := iter.Next(); found {
		fmt.Printf("  Key: %s, Value: %s\n", elem.Key, elem.Value)
	} else {
		fmt.Println("  Iterator je zaustavljen")
	}
}
