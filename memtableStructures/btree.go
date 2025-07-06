package main

import (
	"fmt"
	"sort"
	"strings"
)

type BTreeNode struct {
	keys     []string     // array of keys
	values   []Element    // array of values
	degree   int          // minimum degree
	children []*BTreeNode // children nodes of a current node
	n        int          // current number of keys
	isLeaf   bool
}

type BTree struct {
	root        *BTreeNode // first node
	degree      int        // minimum degree
	lastElement *Element
}

func newBTree(_degree int) *BTree {
	return &BTree{
		root:   nil,
		degree: _degree,
	}
}

func NewBTreeNode(_degree int, _isLeaf bool) *BTreeNode {
	return &BTreeNode{
		keys:     make([]string, 2*_degree-1),
		values:   make([]Element, 2*_degree-1),
		children: make([]*BTreeNode, 2*_degree),
		n:        0,
		isLeaf:   _isLeaf,
		degree:   _degree,
	}
}

func (treeNode *BTreeNode) findKey(k string) int {
	i := 0
	for i < treeNode.n && treeNode.keys[i] < k {
		i++
	}
	return i
}

func (treeNode *BTreeNode) remove(k string) {
	idx := treeNode.findKey(k)

	// Case 1: The key to be removed is present in this node
	if idx < treeNode.n && treeNode.keys[idx] == k {
		if treeNode.isLeaf {
			// If the node is a leaf node, call removeFromLeaf
			treeNode.removeFromLeaf(idx)
		} else {
			// Otherwise, call removeFromNonLeaf
			treeNode.removeFromNonLeaf(idx)
		}
	} else {
		// Case 2: The key to be removed is not present in this node

		// If this node is a leaf, then the key is not present in the B-Tree
		if treeNode.isLeaf {
			fmt.Printf("The key %s does not exist in the tree\n", k)
			return
		}

		// Flag indicating if the key is in the last child
		flag := (idx == treeNode.n)

		// If the child where the key is supposed to exist has less than `t` keys, fill that child
		if treeNode.children[idx].n < treeNode.degree {
			treeNode.fill(idx)
		}

		// Recurse into the appropriate child
		if flag && idx > treeNode.n {
			treeNode.children[idx-1].remove(k)
		} else {
			treeNode.children[idx].remove(k)
		}
	}
}

func (t *BTree) findNewLastElement() *Element {
	current := t.root
	if current == nil {
		return nil
	}

	for !current.isLeaf {
		current = current.children[len(current.children)-1]
	}

	// Iterate backwards to find the last non-tombstoned element
	for i := current.n - 1; i >= 0; i-- {
		if !current.values[i].Tombstone {
			return &current.values[i]
		}
	}

	return nil // No valid element found
}

func (treeNode *BTreeNode) removeFromLeaf(idx int) {
	// Move all keys after the idx-th position one place backward
	for i := idx + 1; i < treeNode.n; i++ {
		treeNode.keys[i-1] = treeNode.keys[i]
		treeNode.values[i-1] = treeNode.values[i]
	}
	// Reduce the count of keys
	treeNode.n--
}

func (treeNode *BTreeNode) removeFromNonLeaf(index int) {
	k := treeNode.keys[index]

	// Case 1: The child that precedes k (C[idx]) has at least t keys
	if treeNode.children[index].n >= treeNode.degree {
		pred := treeNode.getPred(index)
		treeNode.keys[index] = pred
		treeNode.children[index].remove(pred)
	} else if treeNode.children[index+1].n >= treeNode.degree {
		// Case 2: The child that succeeds k (C[idx+1]) has at least t keys
		succ := treeNode.getSucc(index)
		treeNode.keys[index] = succ
		treeNode.children[index+1].remove(succ)
	} else {
		// Case 3: Both C[idx] and C[idx+1] have less than t keys, merge them
		treeNode.merge(index)
		treeNode.children[index].remove(k)
	}
}

func (treeNode *BTreeNode) getPred(idx int) string {
	// Start at the child preceding idx and move to the rightmost leaf node
	cur := treeNode.children[idx]
	for !cur.isLeaf {
		cur = cur.children[cur.n]
	}
	// Return the last key in the rightmost leaf node
	return cur.keys[cur.n-1]
}

func (treeNode *BTreeNode) getSucc(idx int) string {
	// Start at the child following idx and move to the leftmost leaf node
	cur := treeNode.children[idx+1]
	for !cur.isLeaf {
		cur = cur.children[0]
	}
	// Return the first key in the leftmost leaf node
	return cur.keys[0]
}

// BorrowFromPrev borrows a key from children[index-1] and inserts it into children[index].
func (treeNode *BTreeNode) borrowFromPrev(index int) {
	child := treeNode.children[index]
	sibling := treeNode.children[index-1]

	// Move all keys in children[index] one step ahead.
	for i := child.n - 1; i >= 0; i-- {
		child.keys[i+1] = child.keys[i]
		child.values[i+1] = child.values[i]
	}

	// If children[index] is not a leaf, move all its child pointers one step ahead.
	if !child.isLeaf {
		for i := child.n; i >= 0; i-- {
			child.children[i+1] = child.children[i]
		}
	}

	// Set the child's first key equal to keys[index-1] from the current node.
	child.keys[0] = treeNode.keys[index-1]
	child.values[0] = treeNode.values[index-1]

	// Move sibling's last child as children[index]'s first child.
	if !child.isLeaf {
		child.children[0] = sibling.children[sibling.n]
	}

	// Move the key from the sibling to the parent, reducing the number of keys in the sibling.
	treeNode.keys[index-1] = sibling.keys[sibling.n-1]
	treeNode.values[index-1] = sibling.values[sibling.n-1]

	child.n++
	sibling.n--
}

// BorrowFromNext borrows a key from children[index+1] and places it in children[index].
func (treeNode *BTreeNode) borrowFromNext(index int) {
	child := treeNode.children[index]
	sibling := treeNode.children[index+1]

	// Insert keys[index] as the last key in children[index].
	child.keys[child.n] = treeNode.keys[index]
	child.values[child.n] = treeNode.values[index]

	// Insert sibling's first child as the last child into children[index].
	if !child.isLeaf {
		child.children[child.n+1] = sibling.children[0]
	}

	// Set the first key of the sibling into keys[index].
	treeNode.keys[index] = sibling.keys[0]
	treeNode.values[index] = sibling.values[0]

	// Shift all keys in sibling one step behind.
	for i := 1; i < sibling.n; i++ {
		sibling.keys[i-1] = sibling.keys[i]
		sibling.values[i-1] = sibling.values[i]
	}

	// Shift all child pointers in sibling one step behind.
	if !sibling.isLeaf {
		for i := 1; i <= sibling.n; i++ {
			sibling.children[i-1] = sibling.children[i]
		}
	}

	// Update the key counts of children[index] and children[index+1].
	child.n++
	sibling.n--
}

// Merge merges children[index] with children[index+1] and frees children[index+1].
func (treeNode *BTreeNode) merge(index int) {
	child := treeNode.children[index]
	sibling := treeNode.children[index+1]

	// Pull a key from the current node and insert it into the (degree-1)th position of children[index].
	child.keys[treeNode.degree-1] = treeNode.keys[index]
	child.values[treeNode.degree-1] = treeNode.values[index]

	// Copy the keys from children[index+1] to children[index] at the end.
	for i := 0; i < sibling.n; i++ {
		child.keys[i+treeNode.degree] = sibling.keys[i]
		child.values[i+treeNode.degree] = sibling.values[i]
	}

	// Copy the child pointers from children[index+1] to children[index].
	if !child.isLeaf {
		for i := 0; i <= sibling.n; i++ {
			child.children[i+treeNode.degree] = sibling.children[i]
		}
	}

	// Shift all keys after index in the current node one step before.
	for i := index + 1; i < treeNode.n; i++ {
		treeNode.keys[i-1] = treeNode.keys[i]
		treeNode.values[i-1] = treeNode.values[i]
	}

	// Shift all child pointers after (index+1) in the current node one step before.
	for i := index + 2; i <= treeNode.n; i++ {
		treeNode.children[i-1] = treeNode.children[i]
	}

	// Update the key count of child and the current node.
	child.n += sibling.n + 1
	treeNode.n--

	// Free the memory occupied by sibling.
	sibling = nil
}

func (treeNode *BTreeNode) fill(index int) {
	// If the previous child has more than t-1 keys, borrow a key from it
	if index != 0 && treeNode.children[index-1].n >= treeNode.degree {
		treeNode.borrowFromPrev(index)
	} else if index != treeNode.n && treeNode.children[index+1].n >= treeNode.degree {
		// If the next child has more than t-1 keys, borrow a key from it
		treeNode.borrowFromNext(index)
	} else {
		// Merge C[idx] with its sibling
		// If C[idx] is the last child, merge it with its previous sibling
		// Otherwise, merge it with its next sibling
		if index != treeNode.n {
			treeNode.merge(index)
		} else {
			treeNode.merge(index - 1)
		}
	}
}

func (tree *BTree) insert(element Element) {
	if tree.root == nil { // if tree is empty, we initialize root
		tree.root = NewBTreeNode(tree.degree, true)
		tree.root.keys[0] = element.Key
		tree.root.values[0] = element
		tree.root.n = 1
	} else {
		if tree.root.n == 2*tree.degree-1 {
			node := BTreeNode{degree: tree.degree, isLeaf: false}
			node.children[0] = tree.root
			node.splitChild(0, tree.root)
			i := 0
			if node.keys[0] < element.Key {
				i++
			}
			node.children[i].insertNonFull(element)
			tree.root = &node
		} else {
			tree.root.insertNonFull(element)
		}
	}

	// Azuriranje poslednjeg elementa
	if tree.lastElement == nil || element.Timestamp > tree.lastElement.Timestamp {
		tree.lastElement = &element
	}
}

func (treeNode *BTreeNode) insertNonFull(element Element) {
	i := treeNode.n - 1
	if treeNode.isLeaf {
		for i >= 0 && treeNode.keys[i] > element.Key {
			treeNode.keys[i+1] = treeNode.keys[i]
			treeNode.values[i+1] = treeNode.values[i]
			i--
		}
		treeNode.keys[i+1] = element.Key
		treeNode.values[i+1] = element
		treeNode.n += 1
	} else {
		for i >= 0 && treeNode.keys[i] > element.Key {
			i--
		}
		if treeNode.children[i+1].n == 2*treeNode.degree-1 {
			treeNode.splitChild(i+1, treeNode.children[i+1])
			if treeNode.keys[i+1] < element.Key {
				i++
			}
		}
		treeNode.children[i+1].insertNonFull(element)
	}
}

func (treeNode *BTreeNode) splitChild(i int, splitingNode *BTreeNode) {
	deg := treeNode.degree
	node := BTreeNode{degree: splitingNode.degree, isLeaf: splitingNode.isLeaf, n: deg - 1}

	for i := 0; i < deg-1; i++ {
		node.keys[i] = splitingNode.keys[i+deg]
		node.values[i] = splitingNode.values[i+deg]
	}

	if !splitingNode.isLeaf {
		for i := 0; i < deg; i++ {
			node.children[i] = splitingNode.children[i+deg]
		}
	}

	splitingNode.n = deg - 1
	for j := treeNode.n; j >= i+1; j-- {
		treeNode.children[j+1] = treeNode.children[j]
	}
	treeNode.children[i+1] = &node

	for j := treeNode.n - 1; j >= i; j-- {
		treeNode.keys[j+1] = treeNode.keys[j]
		treeNode.values[j+1] = treeNode.values[j]
	}
	treeNode.keys[i] = splitingNode.keys[deg-1]
	treeNode.values[i] = splitingNode.values[deg-1]

	treeNode.n++
}

// func (treeNode *BTreeNode) traverse() {
// 	i := 0
// 	for ; i < treeNode.n; i++ {
// 		if !treeNode.isLeaf {
// 			treeNode.children[i].traverse()
// 		}
// 		fmt.Println("Value of a node ", treeNode.keys[i])
// 	}

// 	if !treeNode.isLeaf {
// 		treeNode.children[i].traverse()
// 	}
// }

func (treeNode *BTreeNode) search(k string) (*Element, bool) {
	i := 0
	for i < treeNode.n && k > treeNode.keys[i] {
		i++
	}

	if i < treeNode.n && treeNode.keys[i] == k {
		fmt.Println("Key", k, "is found")
		return &treeNode.values[i], true
	}

	if treeNode.isLeaf {
		fmt.Println("Key", k, "is not found")
		return nil, false // key not found
	}

	return treeNode.children[i].search(k)
}

func (tree *BTree) remove(k string) {
	if tree.root == nil {
		fmt.Println("Tree is empty")
		return
	}

	tree.root.remove(k)

	if tree.root.n == 0 {
		if tree.root.isLeaf {
			tree.root = nil
		} else {
			tree.root = tree.root.children[0]
		}
	}

	// If the last element is deleted, find the new last element
	if tree.root.isLeaf && tree.root.n > 0 && tree.root.values[tree.root.n-1].Key == k {
		tree.findNewLastElement()
	}
}

func (tree *BTree) update(element Element) {
	if tree.root == nil {
		tree.insert(element)
	} else {
		tree.root.update(element)
	}
}

func (treeNode *BTreeNode) update(element Element) {
	i := 0
	for i < treeNode.n && element.Key > treeNode.keys[i] {
		i++
	}

	if i < treeNode.n && treeNode.keys[i] == element.Key {
		// Ažuriramo vrednost ako ključ već postoji
		treeNode.values[i] = element
	} else {
		if treeNode.isLeaf {
			// Umećemo novi element ako ključ ne postoji
			treeNode.insertNonFull(element)
		} else {
			if treeNode.children[i].n == 2*treeNode.degree-1 {
				treeNode.splitChild(i, treeNode.children[i])
				if treeNode.keys[i] < element.Key {
					i++
				}
			}
			treeNode.children[i].update(element)
		}
	}
}

// func (tree *BTree) traverse() {
// 	// tree.root.traverse()
// 	if tree.root != nil {
// 		tree.root.traverse()
// 	}
// }

func (tree *BTree) search(k string) (*Element, bool) {
	if tree.root == nil {
		return nil, false
	}
	return tree.root.search(k)
}

func (tree *BTree) getAllElements() []*Element {
	var elements []*Element
	if tree.root != nil {
		tree.root.collectElements(&elements)
	}
	return elements
}

func (treeNode *BTreeNode) collectElements(elements *[]*Element) {
	i := 0
	for ; i < treeNode.n; i++ {
		if !treeNode.isLeaf {
			treeNode.children[i].collectElements(elements)
		}
		*elements = append(*elements, &treeNode.values[i])
	}

	if !treeNode.isLeaf {
		treeNode.children[i].collectElements(elements)
	}
}

func (tree *BTree) LastElement() *Element {
	return tree.lastElement
}

func (bt *BTree) searchByPrefix(prefix string) []*Element {
	var results []*Element
	bt.searchPrefixInNode(bt.root, prefix, &results)

	// Sortiramo rezultate po kljucu
	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})

	return results
}

func (bt *BTree) searchPrefixInNode(node *BTreeNode, prefix string, results *[]*Element) {
	if node == nil {
		return
	}

	// Pretraga u trenutnom cvoru
	for i := 0; i < node.n; i++ {
		elem := &node.values[i]
		if strings.HasPrefix(elem.Key, prefix) && !elem.Tombstone {
			*results = append(*results, elem)
		}
	}

	// Rekurzivna pretraga u deci
	if !node.isLeaf {
		for _, child := range node.children {
			bt.searchPrefixInNode(child, prefix, results)
		}
	}
}

func (bt *BTree) searchByRange(startKey, endKey string) []*Element {
	var results []*Element
	bt.searchRangeInNode(bt.root, startKey, endKey, &results)

	// Sortiramo rezultate po kljucu
	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})

	return results
}

func (bt *BTree) searchRangeInNode(node *BTreeNode, startKey, endKey string, results *[]*Element) {
	if node == nil {
		return
	}

	// Pretrazujemo elemente u trenutnom cvoru
	for i := 0; i < node.n; i++ {
		elem := &node.values[i]
		if elem.Key >= startKey && elem.Key <= endKey && !elem.Tombstone {
			*results = append(*results, elem)
		}
	}

	// Rekurzivna pretraga u deci
	if !node.isLeaf {
		for _, child := range node.children {
			bt.searchRangeInNode(child, startKey, endKey, results)
		}
	}
}

// func main() {
// 	tree := newBTree(3)
// 	tree.insert(1)
// 	tree.insert(2)
// 	tree.search(2)
// 	tree.insert(15)
// 	tree.traverse()
// 	tree.remove(2)
// 	tree.traverse()
// 	tree.search(2)
// }
