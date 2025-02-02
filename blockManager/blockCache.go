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

type blockCache struct {
	pl      *priorityList
	nodeMap map[key]*Node
	LENGTH  int
	size    int
}

func InitBlockCache(maxLength int) *blockCache {
	return &blockCache{
		pl:      initPriorityList(),
		nodeMap: make(map[key]*Node),
		LENGTH:  maxLength,
		size:    0,
	}
}

// Adds block, assuming that the block isn't already in cache
func (bc *blockCache) addBlock(block *Block) {
	node := InitNode(block)
	key := initKey(block.GetFilePath(), block.GetOffset())
	bc.nodeMap[key] = node
	bc.pl.AddFirst(node)
	bc.size += 1

	if bc.size > bc.LENGTH {
		bc.size -= 1
		lastNode := bc.pl.RemoveLast()
		delete(bc.nodeMap, lastNode.GetKey())
	}

}

func (bc *blockCache) findBlock(filePath string, offset int) (*Block, bool) {
	node, ok := bc.nodeMap[initKey(filePath, offset)]
	if ok {
		bc.pl.moveUp(node)
		return node.GetBlock(), true
	}
	return nil, false
}
