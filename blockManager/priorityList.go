package blockManager

//Node class used in priorityList class.
//Has pointer to next and previous node.
//It keeps pointer to block as value
type Node struct {
	previous *Node
	next     *Node
	block    *Block
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
	}
}

//PriorityList class.
type priorityList struct {
	head   *Node
	tail   *Node
	length int
	size   int
}

func (pl *priorityList) AddFirst(b *Block) {
	newNode := InitNode(b)
	newNode.SetNext(pl.head)
	newNode.SetPrevious(pl.head.GetPrevious())

	pl.head.SetPrevious(newNode)

	pl.size += 1
	if pl.size > pl.length {
		pl.RemoveLast()
	}
}

func (pl *priorityList) RemoveLast() {
	lastNode := pl.tail.GetNext()
	pl.tail.SetNext(lastNode.GetNext())
	lastNode.GetNext().SetPrevious(pl.tail)
	pl.size -= 1
}

//Moves a node up to head if successful search
func (pl *priorityList) moveUp(node *Node) {
	node.GetPrevious().SetNext(node.GetNext())
	node.GetNext().SetPrevious(node.GetPrevious())

	node.SetNext(pl.head)
	node.SetPrevious(pl.head.GetPrevious())

	node.GetPrevious().SetNext(node)
	pl.head.SetPrevious(node)
}
