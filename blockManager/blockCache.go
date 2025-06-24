package blockManager

type key struct {
	filePath string
	offset   uint64
}

func initKey(filePath string, offset uint64) key {
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
	GetOffset() uint64
	GetData() []byte
}

// Adds block, it block with same path and offset already exists then update and move up
func (bc *blockCache[T]) addBlock(block T) {
	node := InitNode(block)
	key := initKey(block.GetFilePath(), block.GetOffset())
	existingNode, exists := bc.nodeMap[key]
	if exists {
		existingNode.data = node.data
		bc.pl.moveUp(existingNode)
		return
	}
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

// Block is attempted to be accessed from a map. If successful, the node moves up first and returns data
// of node (block)
func (bc *blockCache[T]) findBlock(filePath string, offset uint64) (T, bool) {
	node, ok := bc.nodeMap[initKey(filePath, offset)]
	if ok {
		bc.pl.moveUp(node)
		return node.GetData(), true
	}

	return nil, false
}
