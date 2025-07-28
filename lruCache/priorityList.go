package lruCache

type Node struct {
	previous  *Node
	next      *Node
	key       string
	value     []byte
	tombstone bool
}

func newNode(key string, value []byte) *Node {
	return &Node{key: key, value: value, tombstone: false}
}

type priorityList struct {
	head *Node
	tail *Node
}

func newPriorityList() *priorityList {
	head := &Node{}
	tail := &Node{}
	head.next = tail
	tail.previous = head
	return &priorityList{head: head, tail: tail}
}

func (pl *priorityList) addFirst(node *Node) {
	node.next = pl.tail
	node.previous = pl.tail.previous
	pl.tail.previous.next = node
	pl.tail.previous = node
}

func (pl *priorityList) removeLast() *Node {
	last := pl.head.next
	pl.head.next = last.next
	last.next.previous = pl.head
	return last
}

func (pl *priorityList) remove(node *Node) {
	node.previous.next = node.next
	node.next.previous = node.previous
}

func (pl *priorityList) moveUp(node *Node) {
	pl.remove(node)
	pl.addFirst(node)
}
