package blockManager

type key struct {
	filePath string
	offset   int
}

func initKey(filePath string, offset int) key {
	return key{
		filePath: filePath,
		offset:   offset,
	}
}

type blockCache[T BlockConstraint] struct {
	pl      *priorityList[T]
	nodeMap map[key]*Node[T]
	LENGTH  int
	size    int
}

func InitBlockCache[T BlockConstraint](maxLength int) *blockCache[T] {
	return &blockCache[T]{
		pl:      initPriorityList[T](),
		nodeMap: make(map[key]*Node[T]),
		LENGTH:  maxLength,
		size:    0,
	}
}

type BlockConstraint interface {
	*Block
	GetFilePath() string
	GetOffset() int
	GetData() []byte
}

// Adds block, assuming that the block isn't already in cache
func (bc *blockCache[T]) addBlock(block T) {
	node := InitNode(block)
	key := initKey(block.GetFilePath(), block.GetOffset())
	bc.nodeMap[key] = node
	bc.pl.AddFirst(node)
	bc.size += 1

	if bc.size > bc.LENGTH {
		bc.size -= 1
		lastNode := bc.pl.RemoveLast()
		key = initKey(lastNode.GetData().GetFilePath(), lastNode.GetData().GetOffset())
		delete(bc.nodeMap, key)
	}

}

func (bc *blockCache[T]) findBlock(filePath string, offset int) (T, bool) {
	node, ok := bc.nodeMap[initKey(filePath, offset)]
	if ok {
		bc.pl.moveUp(node)
		return node.GetData(), true
	}
	var zero T
	return zero, false
}
