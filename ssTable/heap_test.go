package ssTable

import (
	"bytes"
	"container/heap"
	"testing"
)

func TestEntryHeapOrdering(t *testing.T) {
	tests := []struct {
		name     string
		input    []*Entry
		expected []*Entry
	}{
		{
			name: "basic ordering",
			input: []*Entry{
				{Key: []byte("delta")},
				{Key: []byte("alpha")},
				{Key: []byte("charlie")},
				{Key: []byte("bravo")},
			},
			expected: []*Entry{
				{Key: []byte("alpha")},
				{Key: []byte("bravo")},
				{Key: []byte("charlie")},
				{Key: []byte("delta")},
			},
		},
		{
			name:     "empty heap",
			input:    []*Entry{},
			expected: []*Entry{},
		},
		{
			name: "duplicates",
			input: []*Entry{
				{Key: []byte("echo")},
				{Key: []byte("echo")},
				{Key: []byte("alpha")},
				{Key: []byte("alpha")},
			},
			expected: []*Entry{
				{Key: []byte("alpha")},
				{Key: []byte("alpha")},
				{Key: []byte("echo")},
				{Key: []byte("echo")},
			},
		},
		{
			name: "single element",
			input: []*Entry{
				{Key: []byte("single")},
			},
			expected: []*Entry{
				{Key: []byte("single")},
			},
		},
	}

	for _, tc := range tests {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			h := &EntryHeap{}
			heap.Init(h)

			for _, e := range tc.input {
				heap.Push(h, e)
			}

			var got []*Entry
			for h.Len() > 0 {
				got = append(got, heap.Pop(h).(*Entry))
			}

			if len(got) != len(tc.expected) {
				t.Fatalf("Expected %d elements, got %d", len(tc.expected), len(got))
			}

			for i := range got {
				if !bytes.Equal(got[i].Key, tc.expected[i].Key) {
					t.Errorf("At index %d: expected key %s, got %s", i, tc.expected[i].Key, got[i].Key)
				}
			}
		})
	}
}
