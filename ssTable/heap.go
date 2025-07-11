package ssTable

import (
	"bytes"
)

// EntryHeap is a min-heap of *Entry ordered by key
// Used for merge during compaction

type EntryHeap []*Entry

func (h EntryHeap) Len() int {
	return len(h)
}

func (h EntryHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].key, h[j].key) < 0
}

func (h EntryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *EntryHeap) Push(x any) {
	*h = append(*h, x.(*Entry))
}

func (h *EntryHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
