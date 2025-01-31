package blockManager

type blockCache struct {
	pl      *priorityList
	nodeMap map[int][]*Node
	LENGTH  int
	size    int
}

func InitBlockCache(maxLength int) *blockCache {
	return &blockCache{
		pl:      initPriorityList(),
		nodeMap: make(map[int][]*Node),
		LENGTH:  maxLength,
		size:    0,
	}
}

func (bc *blockCache) addBlock(keyCrc int, block *Block) {
	node := InitNode(block)
	bc.pl.AddFirst(node)
	bc.size += 1
	_, ok := bc.nodeMap[keyCrc]
	if !ok {
		bc.nodeMap[keyCrc] = make([]*Node, 0)
	}
	bc.nodeMap[keyCrc] = append(bc.nodeMap[keyCrc], node)

	if bc.size > bc.LENGTH {
		bc.size -= 1
		lastNode := bc.pl.RemoveLast()
		for key, nodeList := range bc.nodeMap {
			if nodeList[0] == lastNode {
				bc.nodeMap[key] = nodeList[1:]
			}
		}
	}

}
