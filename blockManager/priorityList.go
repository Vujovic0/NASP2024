package blockManager

//Node class used in priorityList class.
//Has pointer to next and previous node.
//It keeps pointer to block as value
type Node[T any] struct {
	previous *Node[T]
	next     *Node[T]
	data     T
}

func (n *Node[T]) SetPrevious(newNode *Node[T]) {
	n.previous = newNode
}

func (n *Node[T]) GetPrevious() *Node[T] {
	return n.previous
}

func (n *Node[T]) SetNext(newNode *Node[T]) {
	n.next = newNode
}

func (n *Node[T]) GetNext() *Node[T] {
	return n.next
}

func (n *Node[T]) GetData() T {
	return n.data
}

func InitNode[T any](data T) *Node[T] {
	return &Node[T]{
		previous: nil,
		next:     nil,
		data:     data,
	}
}

func InitNodeEmpty[T any]() *Node[T] {
	return &Node[T]{
		previous: nil,
		next:     nil,
	}
}

//PriorityList class.
//Has pointer to first and last node that don't have values.
//Has max length and current size.
type priorityList[T any] struct {
	tail *Node[T]
	head *Node[T]
}

func initPriorityList[T any]() *priorityList[T] {
	pl := &priorityList[T]{
		tail: InitNodeEmpty[T](),
		head: InitNodeEmpty[T](),
	}

	pl.tail.previous = pl.head
	pl.head.next = pl.tail
	return pl
}

func (pl *priorityList[T]) AddFirst(newNode *Node[T]) {
	newNode.SetNext(pl.tail)
	newNode.SetPrevious(pl.tail.GetPrevious())

	pl.tail.GetPrevious().SetNext(newNode)
	pl.tail.SetPrevious(newNode)
}

func (pl *priorityList[T]) RemoveLast() *Node[T] {
	lastNode := pl.head.GetNext()

	pl.head.SetNext(lastNode.GetNext())
	lastNode.GetNext().SetPrevious(pl.head)

	return lastNode
}

func (pl *priorityList[T]) Remove(node *Node[T]) {
	node.GetPrevious().SetNext(node.GetNext())
	node.GetNext().SetPrevious(node.GetPrevious())
}

//Moves a node up to head
func (pl *priorityList[T]) moveUp(node *Node[T]) {
	node.GetPrevious().SetNext(node.GetNext())
	node.GetNext().SetPrevious(node.GetPrevious())

	node.SetNext(pl.tail)
	node.SetPrevious(pl.tail.GetPrevious())

	node.GetPrevious().SetNext(node)
	pl.tail.SetPrevious(node)
}
