package lruCache

type LRUCache struct {
	list    *priorityList
	entries map[string]*Node
	cap     int
	size    int
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		list:    newPriorityList(),
		entries: make(map[string]*Node),
		cap:     capacity,
	}
}

func (c *LRUCache) Put(key, value string) {
	if node, exists := c.entries[key]; exists {
		node.value = value
		c.list.moveUp(node)
		return
	}

	node := newNode(key, value)
	c.entries[key] = node
	c.list.addFirst(node)
	c.size++

	if c.size > c.cap {
		evicted := c.list.removeLast()
		delete(c.entries, evicted.key)
		c.size--
	}
}

func (c *LRUCache) Get(key string) (string, bool) {
	if node, exists := c.entries[key]; exists {
		c.list.moveUp(node)
		return node.value, true
	}
	return "", false
}
