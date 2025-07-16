package memtableStructures

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sort"

	"github.com/Vujovic0/NASP2024/blockManager"
	"github.com/Vujovic0/NASP2024/config"
	"github.com/Vujovic0/NASP2024/probabilisticDataStructures/bloomFilter"
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

func (mtm *MemTableManager) Insert(key string, value []byte, tombstone bool, WALSegmentName string, WALCurrentBlock int, WALByte int) {
	mtm.active.Insert(key, value, tombstone)
	// fmt.Printf("CurrentSize: %d, MaxSize: %d\n", mtm.active.CurrentSize, mtm.active.MaxSize)
	if mtm.active.CurrentSize >= int(mtm.active.MaxSize) {
		// DODAJ POSTAVLJANJE VREDNOSTI ZA WAL LOADING
		mtm.active.WALLastSegment = WALSegmentName
		mtm.active.WALBlockOffset = WALCurrentBlock
		mtm.active.WALByteOffset = WALByte
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

func userScanMenu(mtm *MemTableManager) {
	var answer string
	for {
		fmt.Println("\nIzaberite opciju:")
		fmt.Println("[1] Paginirana pretraga po prefiksu")
		fmt.Println("[2] Paginirana pretraga po opsegu (range)")
		fmt.Println("[3] Izlaz")
		fmt.Print("Unesite broj opcije: ")
		fmt.Scanln(&answer)

		if answer == "1" {
			var prefix string
			var page, size int
			fmt.Print("Unesite prefiks: ")
			fmt.Scanln(&prefix)

			// Unos broja stranice
			for {
				fmt.Print("Unesite broj stranice (ceo broj > 0): ")
				_, err := fmt.Scanln(&page)
				if err == nil && page > 0 {
					break
				}
				fmt.Println("Greska: Broj stranice mora biti ceo broj veci od 0!")
				// Cistimo buffer u slucaju pogresnog unosa
				var discard string
				fmt.Scanln(&discard)
			}

			// Unos velicine stranice
			for {
				fmt.Print("Unesite velicinu stranice (ceo broj > 0): ")
				_, err := fmt.Scanln(&size)
				if err == nil && size > 0 {
					break
				}
				fmt.Println("Greska: Velicina stranice mora biti ceo broj veci od 0!")
				var discard string
				fmt.Scanln(&discard)
			}

			results, found := mtm.PrefixScan(prefix, page, size)
			if found {
				fmt.Printf("Rezultati za stranicu %d (velicina %d):\n", page, size)
				for _, elem := range results {
					fmt.Printf("  Key: %s, Value: %s\n", elem.Key, elem.Value)
				}
			} else {
				fmt.Println("Nema rezultata za dati prefiks ili stranicu.")
			}
		} else if answer == "2" {
			var start, end string
			var page, size int
			fmt.Print("Unesite pocetni kljuc: ")
			fmt.Scanln(&start)
			fmt.Print("Unesite krajnji kljuc: ")
			fmt.Scanln(&end)

			// Unos broja stranice
			for {
				fmt.Print("Unesite broj stranice (ceo broj > 0): ")
				_, err := fmt.Scanln(&page)
				if err == nil && page > 0 {
					break
				}
				fmt.Println("Greska: Broj stranice mora biti ceo broj veci od 0!")
				var discard string
				fmt.Scanln(&discard)
			}

			// Unos velicine stranice
			for {
				fmt.Print("Unesite velicinu stranice (ceo broj > 0): ")
				_, err := fmt.Scanln(&size)
				if err == nil && size > 0 {
					break
				}
				fmt.Println("Greska: Velicina stranice mora biti ceo broj veci od 0!")
				var discard string
				fmt.Scanln(&discard)
			}

			results, found := mtm.RangeScan(KeyRange{StartKey: start, EndKey: end}, page, size)
			if found {
				fmt.Printf("Rezultati za stranicu %d (velicina %d):\n", page, size)
				for _, elem := range results {
					fmt.Printf("  Key: %s, Value: %s\n", elem.Key, elem.Value)
				}
			} else {
				fmt.Println("Nema rezultata za dati opseg ili stranicu.")
			}
		} else if answer == "3" {
			fmt.Println("Izlaz iz paginiranog menija.")
			return
		} else {
			fmt.Println("Pogresan unos, molimo vas da unesete 1, 2 ili 3.")
		}
	}
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

func userSearchMenu(mtm *MemTableManager) {
	var answer string
	for {
		fmt.Println("\nIzaberite opciju:")
		fmt.Println("[1] Pretraga po prefiksu")
		fmt.Println("[2] Pretraga po opsegu (range)")
		fmt.Println("[3] Izlaz")
		fmt.Print("Unesite broj opcije: ")
		fmt.Scanln(&answer)

		if answer == "1" {
			var prefix string
			fmt.Print("Unesite prefiks: ")
			fmt.Scanln(&prefix)
			iter := mtm.PrefixIterate(prefix)
			fmt.Println("Rezultati pretrage po prefiksu:")
			for iter.HasNext() {
				elem, _ := iter.Next()
				fmt.Printf("  Key: %s, Value: %s\n", elem.Key, elem.Value)
			}
		} else if answer == "2" {
			var start, end string
			fmt.Print("Unesite pocetni kljuc: ")
			fmt.Scanln(&start)
			fmt.Print("Unesite krajnji kljuc: ")
			fmt.Scanln(&end)
			iter := mtm.RangeIterate(KeyRange{StartKey: start, EndKey: end})
			fmt.Println("Rezultati pretrage po opsegu:")
			for iter.HasNext() {
				elem, _ := iter.Next()
				fmt.Printf("  Key: %s, Value: %s\n", elem.Key, elem.Value)
			}
		} else if answer == "3" {
			fmt.Println("Izlaz iz pretrage.")
			return
		} else {
			fmt.Println("Pogresan unos, molimo vas da unesete 1, 2 ili 3.")
		}
	}
}

func elementToSSTableElement(elem Element) *ssTable.Element {
	return &ssTable.Element{
		Key:       elem.Key,
		Value:     elem.Value,
		Timestamp: elem.Timestamp,
		Tombstone: elem.Tombstone,
	}
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

		// Kreiramo bloom filter
		filter := bloomFilter.MakeBloomFilter(len(elements), 0.01) // 1% false positive rate
		keys := make([]string, len(elements))
		for i, elem := range elements {
			keys[i] = elem.Key
		}
		bloomFilter.AddData(filter, keys)

		// Kreiramo slice pokazivaca na ssTable.Element
		ssElements := make([]*ssTable.Element, len(elements))
		for i := range elements {
			ssElements[i] = elementToSSTableElement(elements[i])
		}

		// 3. Kreiramo Merkle tree
		merkleTree := ssTable.NewMerkleTree(ssElements)

		// 3. Koristi vrednosti iz konfiguracije
		if !config.SeparateFiles {
			ssTable.CreateCompactSSTable(
				sstableData,
				lastElementData,
				int(config.SummarySparsity),
				int(config.IndexSparsity),
				filter,
				merkleTree,
			)
			fmt.Printf(" SSTable_%d kreiran (COMPACT format) sa %d elemenata\n", tableIndex, len(elements))
		} else {
			ssTable.CreateSeparatedSSTable(
				sstableData,
				lastElementData,
				int(config.SummarySparsity),
				int(config.IndexSparsity),
				filter,
				merkleTree,
			)
			fmt.Printf(" SSTable_%d kreiran (SEPARATED format) sa %d elemenata\n", tableIndex, len(elements))
		}
	} else {
		fmt.Printf(" MemTable_%d je prazna, preskacem flush\n", tableIndex)
	}
}

// Konfigurabilan flush sa parametrima
// func (mtm *MemTableManager) FlushWithConfig(summarySparsity, indexSparsity int, useCompact bool) {
// 	fmt.Printf("Flushujem %d read-only tabela na disk (konfigurabilan)...\n", len(mtm.immutables))

// 	// Ako su parametri 0, koristi vrednosti iz konfiguracije
// 	if summarySparsity == 0 {
// 		summarySparsity = int(config.SummarySparsity)
// 	}
// 	if indexSparsity == 0 {
// 		indexSparsity = int(config.IndexSparsity)
// 	}

// 	for i, memTable := range mtm.immutables {
// 		elements := memTable.Flush()

// 		if len(elements) > 0 {
// 			// 1. Konvertujemo u SSTable format
// 			sstableData := mtm.convertToSSTableFormat(elements)

// 			// 2. Kreiramo boundary element
// 			lastElem := elements[len(elements)-1]
// 			lastEntry := ssTable.InitEntry(0, false, uint64(lastElem.Timestamp), []byte(lastElem.Key), []byte{})
// 			lastElementData := ssTable.SerializeEntry(lastEntry, true)
// 			// 3. Izaberi format na osnovu konfiguracije
// 			if useCompact {
// 				ssTable.CreateCompactSSTable(
// 					sstableData,
// 					lastElementData,
// 					summarySparsity,
// 					indexSparsity,
// 				)
// 				fmt.Printf("SSTable_%d kreiran (COMPACT format) sa %d elemenata\n", i+1, len(elements))
// 			} else {
// 				ssTable.CreateSeparatedSSTable(
// 					sstableData,
// 					lastElementData,
// 					summarySparsity,
// 					indexSparsity,
// 				)
// 				fmt.Printf("SSTable_%d kreiran (SEPARATED format) sa %d elemenata\n", i+1, len(elements))
// 			}
// 		}
// 	}

// 	mtm.immutables = []*MemoryTable{}
// 	fmt.Println("Svi SSTable fajlovi kreirani!")
// }

func (mtm *MemTableManager) FlushActive() {
	fmt.Println("Aktivna memtable je puna...")

	// Dodajemo aktivnu na kraj slice-a (kao najnoviju read-only)
	mtm.immutables = append(mtm.immutables, mtm.active)

	// Ako ima vise od maxImmutables, flushujemo najstariju (prvu)
	if len(mtm.immutables) > mtm.maxImmutables {
		fmt.Println("Najstarija memtable je puna! Flushujemo najstariju na disk...")

		// Uzimamo najstariju (prvu) read-only tabelu
		oldest := mtm.immutables[0]
		// Flushujemo je na disk
		mtm.FlushMemTableToSSTable(oldest, 1)

		// SAVING DATA FOR NEXT PROGRAM STARTUP
		SaveOffsetData(oldest.WALLastSegment, oldest.WALBlockOffset, oldest.WALByteOffset)

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

// // Flush samo najstarije tabele
// func (mtm *MemTableManager) FlushOldest(count int) {
// 	if count > len(mtm.immutables) {
// 		count = len(mtm.immutables)
// 	}

// 	fmt.Printf("Flushujemo najstarije %d tabela na disk...\n", count)

// 	// Uzmi poslednje 'count' tabela (najstarije)
// 	toFlush := mtm.immutables[len(mtm.immutables)-count:]
// 	mtm.immutables = mtm.immutables[:len(mtm.immutables)-count]

// 	for i, memTable := range toFlush {
// 		elements := memTable.Flush()
// 		if len(elements) > 0 {
// 			sstableData := mtm.convertToSSTableFormat(elements)
// 			lastElem := elements[len(elements)-1]
// 			lastEntry := ssTable.InitEntry(0, false, uint64(lastElem.Timestamp), []byte(lastElem.Key), []byte{})
// 			lastElementData := ssTable.SerializeEntry(lastEntry, true)
// 			// Koristi vrednosti iz konfiguracije
// 			ssTable.CreateSeparatedSSTable(
// 				sstableData,
// 				lastElementData,
// 				int(config.SummarySparsity), // Iz konfiguracije
// 				int(config.IndexSparsity),   // Iz konfiguracije
// 			)
// 			fmt.Printf("SSTable_%d kreiran sa %d elemenata\n", i+1, len(elements))
// 		}
// 	}

// 	fmt.Println("Najstarije tabele flush-ovane!")
// }

func elementToEntry(elem Element) *ssTable.Entry {
	return ssTable.InitEntry(0, elem.Tombstone, uint64(elem.Timestamp), []byte(elem.Key), elem.Value)
}

func SaveOffsetData(WALLastSegment string, WALBlockOffset int, WALByteOffset int) {
	nameLen := uint32(len(WALLastSegment))
	// NAME LENGHT 4B | NAME XB | BLOCK INDEX 4B | BYTE INDEX 8B
	arraySize := 4 + nameLen + 4 + 8

	data := make([]byte, arraySize)

	// WRITING NAME LENGHT AND NAME ITSELF
	binary.LittleEndian.PutUint32(data[:4], nameLen)
	copy(data[4:4+nameLen], []byte(WALLastSegment))

	// WRITING BLOCK AND BYTE OFFSET
	binary.LittleEndian.PutUint32(data[4+nameLen:8+nameLen], uint32(WALBlockOffset))
	binary.LittleEndian.PutUint64(data[8+nameLen:], uint64(WALByteOffset))

	dataSize := uint32(len(data))
	blockData := make([]byte, config.GlobalBlockSize)
	blockType := make([]byte, 1)
	blockType[0] = 0
	blockData[4] = blockType[0]
	binary.LittleEndian.PutUint32(blockData[5:9], dataSize)
	copy(blockData[9:], data)
	crc := crc32.ChecksumIEEE(blockData[4:])
	binary.LittleEndian.PutUint32(blockData[0:4], crc)
	block := blockManager.InitBlock("./wal/offset.bin", 0, blockData)

	file, err := os.OpenFile("./wal/offset.bin", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 066)
	if err != nil {
		// OFFSET.BIN PROBLEM
	}

	blockManager.WriteBlock(file, block)
	file.Sync()
	file.Close()
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
		mtm.Insert(key, []byte(value), false, "", 0, 0)
		fmt.Printf("Insertovan: %s -> %s\n", key, value)
	}

	// userScanMenu(mtm)
	userSearchMenu(mtm)
}
