package bloomFilter

import (
	"testing"
)

func TestBloomFilterBasic(t *testing.T) {
	bf := MakeBloomFilter(100, 0.01)

	data := []string{"apple", "banana", "carrot"}
	AddData(bf, data)

	for _, word := range data {
		if !SearchData(bf, word) {
			t.Errorf("Expected %s to be found in Bloom filter", word)
		}
	}

	notPresent := []string{"x", "y", "z"}
	falseCount := 0
	for _, word := range notPresent {
		if SearchData(bf, word) {
			falseCount++
		}
	}
	if falseCount > len(notPresent) {
		t.Errorf("Too many false positives")
	}
}

func TestBloomFilterSerialization(t *testing.T) {
	bf := MakeBloomFilter(100, 0.01)
	data := []string{"grape", "melon", "cherry"}
	AddData(bf, data)

	serialized, err := SerializeToBytes(bf)
	if err != nil {
		t.Fatalf("Serialization failed: %v", err)
	}

	deserialized, err := DeserializeFromBytes(serialized)
	if err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}

	for _, word := range data {
		if !SearchData(deserialized, word) {
			t.Errorf("Expected %s to be found in deserialized Bloom filter", word)
		}
	}
}
