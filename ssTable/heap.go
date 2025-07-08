package ssTable

import "bytes"

type EntryHeap []*Entry

func (eh EntryHeap) Len() int {
	return len(eh)
}

func (eh EntryHeap) Less(i, j int) bool {
	return bytes.Compare(eh[i].Key, eh[j].Key) < 0
}

func (eh EntryHeap) Swap(i, j int) {
	eh[i], eh[j] = eh[j], eh[i]
}

func (eh *EntryHeap) Push(x interface{}) {
	*eh = append(*eh, x.(*Entry)) // moraš pretvoriti u *Entry, a ne string
}

func (eh *EntryHeap) Pop() interface{} {
	old := *eh
	n := len(old)
	x := old[n-1]
	*eh = old[0 : n-1]
	return x
}
