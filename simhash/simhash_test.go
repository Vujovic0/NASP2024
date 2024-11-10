package simhash

import (
	"fmt"
	"testing"
)

func TestGetTokenCount(t *testing.T) {
	// Test case 1: Token count for a simple sentence
	text := "hello world hello"
	result := getTokenCount(text)
	expected := map[string]int{
		"hello": 2,
		"world": 1,
	}

	for key, value := range expected {
		if result[key] != value {
			t.Errorf("Expected token count %d for key %s, but got %d", value, key, result[key])
		}
	}

	// Test case 2: Token count for an empty string
	text = ""
	result = getTokenCount(text)
	expectedEmpty := map[string]int{}
	if len(result) != len(expectedEmpty) {
		t.Errorf("Expected empty map, but got %v", result)
	}
}

func TestGetTokenHash(t *testing.T) {
	// Test case 1: Get token hashes for a set of tokens
	tokenCountMap := map[string]int{
		"hello": 2,
		"world": 1,
	}
	result := getTokenHash(tokenCountMap)
	if len(result) != 2 {
		t.Errorf("Expected 2 tokens in the hash map, but got %d", len(result))
	}

	// Test case 2: Check if the tokens are hashed properly (length of the byte array should be 16)
	for token, hash := range result {
		if len(hash) != 16 {
			t.Errorf("Expected 16 bytes for token %s, but got %d bytes", token, len(hash))
		}
	}
}

func TestGetFingerPrint(t *testing.T) {
	// Test case 1: Check the fingerprint for a sample text
	text := "hello world hello"
	result := GetFingerPrint(text)
	for _, b := range result {
		fmt.Printf("%08b ", b) // %08b prints the byte in 8-bit binary form
	}
	// The expected fingerprint here depends on the hashing and should be generated manually
	// or checked by a known value. For now, we'll just check the length of the result.
	if len(result) != 16 {
		t.Errorf("Expected a fingerprint with 16 bytes, but got %d bytes", len(result))
	}
}

func TestGetDistanceSimHash(t *testing.T) {
	// Test case 1: Compare two identical hashes (expect distance to be 0)
	simHash1 := [16]byte{0xF0, 0x1A, 0x2B, 0x3C, 0x4D, 0x5E, 0x6F, 0x7A, 0x8B, 0x9C, 0xAD, 0xBE, 0xCF, 0xD0, 0xE1, 0xF2}
	simHash2 := simHash1
	result := GetDistanceSimHash(simHash1, simHash2)
	if result != 0 {
		t.Errorf("Expected distance 0, but got %d", result)
	}

	// Test case 2: Compare two different hashes (expect a non-zero distance)
	simHash2 = [16]byte{0xF0, 0x1B, 0x2B, 0x3C, 0x4D, 0x5E, 0x6F, 0x7A, 0x8B, 0x9C, 0xAD, 0xBE, 0xCF, 0xD0, 0xE1, 0xF3}
	result = GetDistanceSimHash(simHash1, simHash2)
	if result == 0 {
		t.Errorf("Expected non-zero distance, but got %d", result)
	}

	simHash1 = GetFingerPrint(`Like all forests, the wooded stretches of the Arctic sometimes catch on fire. But unlike many forests in the mid-latitudes, which thrive on or even require fire to preserve their health, Arctic forests have evolved to burn only infrequently.
Climate change is reshaping that regime. In the first decade of the new millennium, fires burned 50 percent more acreage each year in the Arctic, on average, than any decade in the 1900s. Between 2010 and 2020, burned acreage continued to creep up, particularly in Alaska, which had its second worst fire year ever in 2015 and another bad one in 2019. Scientists have found that fire frequency today is higher than at any time since the formation of boreal forests some 3,000 years ago, and potentially higher than at any point in the last 10,000 years.
Fires in boreal forests can release even more carbon than similar fires in places like California or Europe, because the soils underlying the high-latitude forests are often made of old, carbon-rich peat. In 2020, Arctic fires released almost 250 megatons of carbon dioxide, about half as much as Australia emits in a year from human activities and about 2.5 times as much as the record-breaking 2020 California wildfire season.`)
	simHash2 = GetFingerPrint(`The Amazon rainforest is most likely now a net contributor to warming of the planet, according to a first-of-its-kind analysis from more than 30 scientists.
For years, researchers have expressed concern that rising temperatures, drought, and deforestation are reducing the capacity of the world’s largest rainforest to absorb carbon dioxide from the atmosphere, and help offset emissions from fossil-fuel burning. Recent studies have even suggested that some portions of the tropical landscape already may release more carbon than they store.
But the inhaling and exhaling of CO2 is just one way this damp jungle, the most species-rich on Earth, influences the global climate. Activities in the Amazon, both natural and human-caused, can shift the rainforest’s contribution in significant ways, warming the air directly or releasing other greenhouse gases that do.
Drying wetlands and soil compaction from logging, for example, can increase emissions of the greenhouse gas nitrous oxide. Land-clearing fires release black carbon, small particles of soot that absorb sunlight and increase warmth. Deforestation can alter rainfall patterns, further drying and heating the forest. Regular flooding and dam-building releases the potent gas methane, as does cattle ranching, one chief reason forests are destroyed. And roughly 3.5 percent of all methane released globally comes naturally from the Amazon’s trees.`)
	result = GetDistanceSimHash(simHash1, simHash2)
	for _, b := range simHash1 {
		fmt.Printf("%08b ", b) // %08b prints the byte in 8-bit binary form
	}
	fmt.Println("-----------------------------------")
	for _, b := range simHash2 {
		fmt.Printf("%08b ", b) // %08b prints the byte in 8-bit binary form
	}
	fmt.Print(result)
}
