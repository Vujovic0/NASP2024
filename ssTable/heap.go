package ssTable

import (
	"bytes"
)

/*
	Implementira ugradjen heap interfejs
*/

type EntryHeap []string

func (h EntryHeap) Len() int {
	return len(h)
}

func (h EntryHeap) Less(i, j int) bool {
	return bytes.Compare([]byte(h[i]), []byte(h[j])) < 0
}

func (h EntryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *EntryHeap) Push(x any) {
	*h = append(*h, x.(string))
}

func (h *EntryHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
