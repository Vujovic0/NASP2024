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
				{key: []byte("delta")},
				{key: []byte("alpha")},
				{key: []byte("charlie")},
				{key: []byte("bravo")},
			},
			expected: []*Entry{
				{key: []byte("alpha")},
				{key: []byte("bravo")},
				{key: []byte("charlie")},
				{key: []byte("delta")},
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
				{key: []byte("echo")},
				{key: []byte("echo")},
				{key: []byte("alpha")},
				{key: []byte("alpha")},
			},
			expected: []*Entry{
				{key: []byte("alpha")},
				{key: []byte("alpha")},
				{key: []byte("echo")},
				{key: []byte("echo")},
			},
		},
		{
			name: "single element",
			input: []*Entry{
				{key: []byte("single")},
			},
			expected: []*Entry{
				{key: []byte("single")},
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
				if !bytes.Equal(got[i].key, tc.expected[i].key) {
					t.Errorf("At index %d: expected key %s, got %s", i, tc.expected[i].key, got[i].key)
				}
			}
		})
	}
}
