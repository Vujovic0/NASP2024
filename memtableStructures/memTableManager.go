package memtableStructures

import (
	"fmt"
	"sort"

	"github.com/Vujovic0/NASP2024/config"
	"github.com/Vujovic0/NASP2024/ssTable"
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

// Staroo
// func (mtm *MemTableManager) FlushActive() {
// 	fmt.Println("Aktivna memtable je puna...")

// 	// Prebacujemo aktivnu tabelu u read-only
// 	mtm.immutables = append([]*MemoryTable{mtm.active}, mtm.immutables...)

// 	// Sada proveravamo da li ih ima n (n-1 read-only + 1 aktivna koja je sada postala read-only)
// 	if len(mtm.immutables) > mtm.maxImmutables {
// 		fmt.Println("SVE tabele su pune! Flushujem SVE na disk...")
// 		mtm.FlushAll()
// 	}

// 	mtm.active = initializeMemoryTable()
// }

// func (mtm *MemTableManager) convertToSSTableFormat(elements []Element) []byte {
// 	var data []byte

// 	for _, elem := range elements {
// 		entry := make([]byte, 0)

// 		// CRC (4 bytes) - calculate later
// 		entry = append(entry, make([]byte, 4)...)

// 		// Timestamp (8 bytes)
// 		timestampBytes := make([]byte, 8)
// 		binary.LittleEndian.PutUint64(timestampBytes, uint64(elem.Timestamp))
// 		entry = append(entry, timestampBytes...)

// 		// Tombstone (1 byte)
// 		if elem.Tombstone {
// 			entry = append(entry, 1)
// 		} else {
// 			entry = append(entry, 0)
// 		}

// 		// Key size (8 bytes)
// 		keySizeBytes := make([]byte, 8)
// 		binary.LittleEndian.PutUint64(keySizeBytes, uint64(len(elem.Key)))
// 		entry = append(entry, keySizeBytes...)

// 		if !elem.Tombstone {
// 			// Value size (8 bytes) - only for non-tombstone
// 			valueSizeBytes := make([]byte, 8)
// 			binary.LittleEndian.PutUint64(valueSizeBytes, uint64(len(elem.Value)))
// 			entry = append(entry, valueSizeBytes...)
// 		}

// 		// Key
// 		entry = append(entry, []byte(elem.Key)...)

// 		if !elem.Tombstone {
// 			// Value - only for non-tombstone
// 			entry = append(entry, elem.Value...)
// 		}

// 		// Calculate CRC for everything except first 4 bytes
// 		crc := crc32.ChecksumIEEE(entry[4:])
// 		binary.LittleEndian.PutUint32(entry[0:4], crc)

// 		data = append(data, entry...)
// 	}

//		return data
//	}
//
//	func (mtm *MemTableManager) convertToSSTableFormat(elements []Element) []byte {
//		var data []byte
//		for _, elem := range elements {
//			data = append(data, ssTable.SerializeEntry(
//				elem.Key,
//				string(elem.Value),
//				elem.Tombstone,
//				false, // keyOnly = false
//			)...)
//		}
//		return data
//	}
func (mtm *MemTableManager) convertToSSTableFormat(elements []Element) []byte {
	var data []byte
	for _, elem := range elements {
		entry := elementToEntry(elem)
		data = append(data, ssTable.SerializeEntry(entry, false)...)
	}
	return data
}

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

func (mtm *MemTableManager) FlushAll() {
	fmt.Printf("Flushujemo %d read-only tabela na disk...\n", len(mtm.immutables))

	for i, memTable := range mtm.immutables {
		elements := memTable.Flush()
		if len(elements) > 0 {
			sstableData := mtm.convertToSSTableFormat(elements)
			lastElem := elements[len(elements)-1]
			lastEntry := ssTable.InitEntry(0, false, uint64(lastElem.Timestamp), []byte(lastElem.Key), []byte{})
			lastElementData := ssTable.SerializeEntry(lastEntry, true)

			ssTable.CreateCompactSSTable(
				sstableData,
				lastElementData,
				int(config.SummarySparsity),
				int(config.IndexSparsity),
			)
			// ili
			// ssTable.CreateSeparatedSSTable(...)

			fmt.Printf(" SSTable_%d kreiran (Compact format) sa %d elemenata (SummarySparsity=%d, IndexSparsity=%d)\n",
				i+1, len(elements), config.SummarySparsity, config.IndexSparsity)
		}
	}

	mtm.immutables = []*MemoryTable{}
	fmt.Println(" Svi SSTable fajlovi kreirani!")
}

// Nova funkcija za flush jedne memtable
func (mtm *MemTableManager) FlushMemTableToSSTable(memTable *MemoryTable, tableIndex int) {
	fmt.Printf("Flushujemo MemTable_%d na disk...\n", tableIndex)

	elements := memTable.Flush()

	if len(elements) > 0 {
		// 1. Konvertujemo u SSTable format
		sstableData := mtm.convertToSSTableFormat(elements)

		// 2. Kreiramo boundary element (poslednji kljuc)
		lastElem := elements[len(elements)-1]
		lastEntry := ssTable.InitEntry(0, false, uint64(lastElem.Timestamp), []byte(lastElem.Key), []byte{})
		lastElementData := ssTable.SerializeEntry(lastEntry, true)
		// 3. Koristi vrednosti iz konfiguracije
		ssTable.CreateSeparatedSSTable(
			sstableData,                 // Glavni podaci u SSTable formatu
			lastElementData,             // Boundary element (poslednji kljuc)
			int(config.SummarySparsity), // Iz konfiguracije
			int(config.IndexSparsity),   // Iz konfiguracije
		)

		fmt.Printf(" SSTable_%d kreiran sa %d elemenata\n", tableIndex, len(elements))
	} else {
		fmt.Printf(" MemTable_%d je prazna, preskacem flush\n", tableIndex)
	}
}

// Konfigurabilan flush sa parametrima
func (mtm *MemTableManager) FlushWithConfig(summarySparsity, indexSparsity int, useCompact bool) {
	fmt.Printf("Flushujem %d read-only tabela na disk (konfigurabilan)...\n", len(mtm.immutables))

	// Ako su parametri 0, koristi vrednosti iz konfiguracije
	if summarySparsity == 0 {
		summarySparsity = int(config.SummarySparsity)
	}
	if indexSparsity == 0 {
		indexSparsity = int(config.IndexSparsity)
	}

	for i, memTable := range mtm.immutables {
		elements := memTable.Flush()

		if len(elements) > 0 {
			// 1. Konvertujemo u SSTable format
			sstableData := mtm.convertToSSTableFormat(elements)

			// 2. Kreiramo boundary element
			lastElem := elements[len(elements)-1]
			lastEntry := ssTable.InitEntry(0, false, uint64(lastElem.Timestamp), []byte(lastElem.Key), []byte{})
			lastElementData := ssTable.SerializeEntry(lastEntry, true)
			// 3. Izaberi format na osnovu konfiguracije
			if useCompact {
				ssTable.CreateCompactSSTable(
					sstableData,
					lastElementData,
					summarySparsity,
					indexSparsity,
				)
				fmt.Printf("SSTable_%d kreiran (COMPACT format) sa %d elemenata\n", i+1, len(elements))
			} else {
				ssTable.CreateSeparatedSSTable(
					sstableData,
					lastElementData,
					summarySparsity,
					indexSparsity,
				)
				fmt.Printf("SSTable_%d kreiran (SEPARATED format) sa %d elemenata\n", i+1, len(elements))
			}
		}
	}

	mtm.immutables = []*MemoryTable{}
	fmt.Println("Svi SSTable fajlovi kreirani!")
}

func (mtm *MemTableManager) FlushActive() {
	fmt.Println("Aktivna memtable je puna...")

	// Dodajemo aktivnu na kraj slice-a (kao najnoviju read-only)
	mtm.immutables = append(mtm.immutables, mtm.active)

	// Ako ima vise od maxImmutables, flushujemo najstariju (prvu)
	if len(mtm.immutables) > mtm.maxImmutables {
		fmt.Println("Najstarija memtable je puna! Flushujem najstariju na disk...")

		// Uzimamo najstariju (prvu) read-only tabelu
		oldest := mtm.immutables[0]
		// Flushujemo je na disk
		mtm.FlushMemTableToSSTable(oldest, 1)
		// Uklanjamo je iz slice-a
		mtm.immutables = mtm.immutables[1:]
		// Koristimo je kao novu aktivnu
		oldest.Reset()
		mtm.active = oldest
	} else {
		// Ako nema potrebe za flush-om, samo napravimo novu praznu aktivnu tabelu
		mtm.active = initializeMemoryTable()
	}
}

// Flush samo najstarije tabele
func (mtm *MemTableManager) FlushOldest(count int) {
	if count > len(mtm.immutables) {
		count = len(mtm.immutables)
	}

	fmt.Printf("Flushujemo najstarije %d tabela na disk...\n", count)

	// Uzmi poslednje 'count' tabela (najstarije)
	toFlush := mtm.immutables[len(mtm.immutables)-count:]
	mtm.immutables = mtm.immutables[:len(mtm.immutables)-count]

	for i, memTable := range toFlush {
		elements := memTable.Flush()
		if len(elements) > 0 {
			sstableData := mtm.convertToSSTableFormat(elements)
			lastElem := elements[len(elements)-1]
			lastEntry := ssTable.InitEntry(0, false, uint64(lastElem.Timestamp), []byte(lastElem.Key), []byte{})
			lastElementData := ssTable.SerializeEntry(lastEntry, true)
			// Koristi vrednosti iz konfiguracije
			ssTable.CreateSeparatedSSTable(
				sstableData,
				lastElementData,
				int(config.SummarySparsity), // Iz konfiguracije
				int(config.IndexSparsity),   // Iz konfiguracije
			)
			fmt.Printf("SSTable_%d kreiran sa %d elemenata\n", i+1, len(elements))
		}
	}

	fmt.Println("ajstarije tabele flush-ovane!")
}
func elementToEntry(elem Element) *ssTable.Entry {
	return ssTable.InitEntry(0, elem.Tombstone, uint64(elem.Timestamp), []byte(elem.Key), elem.Value)
}

func main() {
	mtm := NewMemTableManager(2)

	fmt.Println(" TESTIRANJE FLUSH MEHANIZMA ")

	// Dodajemo dovoljno podataka da se pokrene flush
	testData := []string{
		"apple", "application", "apply", "approach", "approve",
		"banana", "berry", "bird", "book", "bottle",
		"cat", "car", "card", "center", "chair",
	}

	for i, key := range testData {
		value := fmt.Sprintf("value_%d", i+1)
		mtm.Insert(key, []byte(value))
		fmt.Printf("Insertovan: %s -> %s\n", key, value)
	}

	fmt.Println("\n MANUAL FLUSH TEST ")

	// Manual flush test sa config vrednostima
	if len(mtm.immutables) > 0 {
		fmt.Println("Pozivam FlushWithConfig sa default config vrednostima...")
		mtm.FlushWithConfig(0, 0, false) // 0 znaci koristi config vrednosti
	}

	fmt.Println("\n FINALNI TEST ")

	// Test pretrage nakon flush-a
	results, found := mtm.PrefixScan("app", 1, 5)
	if found {
		fmt.Println("Rezultati pretrage nakon flush-a:")
		for _, elem := range results {
			fmt.Printf("  Key: %s, Value: %s\n", elem.Key, elem.Value)
		}
	} else {
		fmt.Println("Nema rezultata za dati prefix nakon flush-a.")
	}
}
