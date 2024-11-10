package simhash

import (
	"crypto/md5"
	"fmt"
	"strings"
)

func GetHashAsString(data []byte) string {
	hash := md5.Sum(data)
	res := ""
	for _, b := range hash {
		res = fmt.Sprintf("%s%b", res, b)
	}
	return res
}

func getHashAsByteArray(data []byte) [16]byte {
	return md5.Sum(data)
}

func Test() {
	fmt.Println(GetHashAsString([]byte("Hello")))
}

func getTokenCount(text string) map[string]int {
	tokenCountMap := make(map[string]int)
	textSliced := strings.Split(text, " ")
	// brojimo koliko imamo puta neku rec unutar teksta
	for _, textSlice := range textSliced {
		textSlice = strings.TrimSpace(textSlice)
		if textSlice == "" {
			continue
		}
		value, ok := tokenCountMap[textSlice]
		if !ok {
			tokenCountMap[textSlice] = 1
			continue
		} else {
			tokenCountMap[textSlice] = value + 1
		}
	}
	return tokenCountMap
}

func getTokenHash(tokenCountMap map[string]int) map[string][16]byte {
	tokenHashMap := make(map[string][16]byte)
	for key := range tokenCountMap {
		tokenHashMap[key] = getHashAsByteArray([]byte(key))
	}
	return tokenHashMap
}

// Vraca simhash vrednost nekog teksta
func GetFingerPrint(text string) [16]byte {
	tokenCountMap := getTokenCount(text)
	tokenHashMap := getTokenHash(tokenCountMap)
	tokenSum := make([]int, 128)
	var bitCounter byte
	for key := range tokenHashMap {
		bitCounter = 0
		for _, byte := range tokenHashMap[key] {
			for i := 7; i >= 0; i-- {
				bit := byte & (1 << i)
				if bit == 0 {
					tokenSum[bitCounter] -= tokenCountMap[key]
				} else {
					tokenSum[bitCounter] += tokenCountMap[key]
				}
				bitCounter++
			}
		}
		if bitCounter > 128 {
			panic("BitCounter tried to go over 128")
		}
	}

	var byteIndex int8
	var bitIndex int8
	var fingerPrint [16]byte
	for index, value := range tokenSum {
		byteIndex = int8(index / 8)
		bitIndex = int8(index % 8)
		if value > 0 {
			fingerPrint[byteIndex] = fingerPrint[byteIndex] | (1 << byte(bitIndex))
		}
	}
	return fingerPrint
}

func GetDistanceSimHash(simHash1 [16]byte, simHash2 [16]byte) int {
	var distanceHash [16]byte
	var distanceLength int
	for i := 0; i < 16; i++ {
		distanceHash[i] = simHash1[i] ^ simHash2[i]
		for bit := 7; bit >= 0; bit-- {
			if distanceHash[i]&(1<<bit) > 0 {
				distanceLength++
			}
		}
	}
	return distanceLength
}
