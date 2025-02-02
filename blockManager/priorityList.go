package blockManager

//Node class used in priorityList class.
//Has pointer to next and previous node.
//It keeps pointer to block as value
type Node struct {
	previous *Node
	next     *Node
	block    *Block
	key      key
}

func (n *Node) SetPrevious(newNode *Node) {
	n.previous = newNode
}

func (n *Node) GetPrevious() *Node {
	return n.previous
}

func (n *Node) SetNext(newNode *Node) {
	n.next = newNode
}

func (n *Node) GetNext() *Node {
	return n.next
}

func (n *Node) GetBlock() *Block {
	return n.block
}

func InitNode(b *Block) *Node {
	return &Node{
		previous: nil,
		next:     nil,
		block:    b,
		key:      initKey(b.GetFilePath(), b.GetOffset()),
	}
}

func InitNodeEmpty() *Node {
	return &Node{
		previous: nil,
		next:     nil,
		block:    nil,
	}
}

func (n *Node) GetKey() key {
	return n.key
}

//PriorityList class.
//Has pointer to first and last node that don't have values.
//Has max length and current size.
type priorityList struct {
	head *Node
	tail *Node
}

func initPriorityList() *priorityList {
	pl := &priorityList{
		head: InitNodeEmpty(),
		tail: InitNodeEmpty(),
	}

	pl.head.previous = pl.tail
	pl.tail.next = pl.head
	return pl
}

func (pl *priorityList) AddFirst(newNode *Node) {
	newNode.SetNext(pl.head)
	newNode.SetPrevious(pl.head.GetPrevious())

	pl.head.SetPrevious(newNode)
}

func (pl *priorityList) RemoveLast() *Node {
	lastNode := pl.tail.GetNext()

	pl.tail.SetNext(lastNode.GetNext())
	lastNode.GetNext().SetPrevious(pl.tail)

	return lastNode
}

//Moves a node up to head
func (pl *priorityList) moveUp(node *Node) {
	node.GetPrevious().SetNext(node.GetNext())
	node.GetNext().SetPrevious(node.GetPrevious())

	node.SetNext(pl.head)
	node.SetPrevious(pl.head.GetPrevious())

	node.GetPrevious().SetNext(node)
	pl.head.SetPrevious(node)
}
