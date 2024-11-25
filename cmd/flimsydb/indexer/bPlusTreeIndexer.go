package indexer

import (
	"fmt"
	"sort"
	"sync"

	cm "github.com/ndgde/flimsy-db/cmd/flimsydb/common"
)

type ValPtrsBunch struct {
	val  cm.Blob
	ptrs []int
}

func NewValPtrsBunch(val cm.Blob, ptr int) ValPtrsBunch {
	return ValPtrsBunch{
		val:  val,
		ptrs: []int{ptr},
	}
}

type Node struct {
	bunches  []ValPtrsBunch
	children []*Node
	isLeaf   bool
	parent   *Node
}

func NewNode(degree int, isLeaf bool, parent *Node) *Node {
	return &Node{
		bunches:  make([]ValPtrsBunch, 0, degree-1),
		children: make([]*Node, 0, degree),
		isLeaf:   isLeaf,
		parent:   parent,
	}
}

type BTreeIndexer struct {
	mu          sync.RWMutex
	root        *Node
	degree      int
	compareFunc cm.CompareFunc
}

func NewBTreeIndexer(valueType cm.TabularType, degree int) *BTreeIndexer {
	if degree < 3 {
		panic("degree must be at least 3")
	}

	return &BTreeIndexer{
		root:        nil,
		degree:      degree,
		compareFunc: cm.GetCompareFunc(valueType),
	}
}

func (bt *BTreeIndexer) Add(val cm.Blob, ptr int) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	node, idx := bt.search(val)
	if node != nil {
		node.bunches[idx].ptrs = append(node.bunches[idx].ptrs, ptr)
		return nil
	}

	if bt.root == nil {
		bt.root = NewNode(bt.degree, true, nil)
		bt.root.bunches = append(bt.root.bunches, NewValPtrsBunch(val, ptr))
		return nil
	}

	node = bt.root
	for !node.isLeaf {
		i := sort.Search(len(node.bunches), func(i int) bool {
			return bt.compareFunc(node.bunches[i].val, val) >= 0
		})
		node = node.children[i]
	}

	i := sort.Search(len(node.bunches), func(i int) bool {
		return bt.compareFunc(node.bunches[i].val, val) >= 0
	})
	node.bunches = append(node.bunches[:i], append([]ValPtrsBunch{NewValPtrsBunch(val, ptr)}, node.bunches[i:]...)...)

	if len(node.bunches) == bt.degree {
		bt.splitNode(node)
	}

	return nil
}

func (bt *BTreeIndexer) splitNode(node *Node) {
	midIndex := (len(node.bunches) - 1) / 2
	midBunch := node.bunches[midIndex]

	left := NewNode(bt.degree, node.isLeaf, node.parent)
	right := NewNode(bt.degree, node.isLeaf, node.parent)

	left.bunches = append(left.bunches, node.bunches[:midIndex]...)
	right.bunches = append(right.bunches, node.bunches[midIndex+1:]...)

	if !node.isLeaf {
		left.children = append(left.children, node.children[:midIndex+1]...)
		right.children = append(right.children, node.children[midIndex+1:]...)
		for _, child := range left.children {
			child.parent = left
		}
		for _, child := range right.children {
			child.parent = right
		}
	}

	if node.parent == nil {
		bt.root = NewNode(bt.degree, false, nil)
		bt.root.bunches = append(bt.root.bunches, midBunch)
		bt.root.children = []*Node{left, right}
		left.parent = bt.root
		right.parent = bt.root
	} else {
		parent := node.parent
		i := sort.Search(len(parent.bunches), func(i int) bool {
			return bt.compareFunc(parent.bunches[i].val, midBunch.val) >= 0
		})
		parent.bunches = append(parent.bunches[:i], append([]ValPtrsBunch{midBunch}, parent.bunches[i:]...)...)
		parent.children = append(parent.children[:i+1], append([]*Node{nil}, parent.children[i+1:]...)...)
		parent.children[i] = left
		parent.children[i+1] = right
		left.parent = parent
		right.parent = parent

		if len(parent.bunches) == bt.degree {
			bt.splitNode(parent)
		}
	}
}

func (bt *BTreeIndexer) Delete(val cm.Blob, ptr int) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	node, idx := bt.search(val)
	if node == nil {
		return fmt.Errorf("value does not exist: %v", val)
	}

	ptrs := node.bunches[idx].ptrs
	ptrIndex := -1
	for i, p := range ptrs {
		if p == ptr {
			ptrIndex = i
			break
		}
	}
	if ptrIndex == -1 {
		return fmt.Errorf("ptr does not exist: %d", ptr)
	}

	node.bunches[idx].ptrs = append(ptrs[:ptrIndex], ptrs[ptrIndex+1:]...)

	if len(node.bunches[idx].ptrs) == 0 {
		node.bunches = append(node.bunches[:idx], node.bunches[idx+1:]...)
		bt.rebalanceAfterDeletion(node)
	}

	return nil
}

func (bt *BTreeIndexer) rebalanceAfterDeletion(node *Node) {
	if node == bt.root && len(node.bunches) == 0 {
		if len(node.children) > 0 {
			bt.root = node.children[0]
			bt.root.parent = nil
		} else {
			bt.root = nil
		}
		return
	}

	if len(node.bunches) >= (bt.degree-1)/2 {
		return
	}

	parent := node.parent
	if parent == nil {
		return
	}

	var nodeIdx int
	for i, child := range parent.children {
		if child == node {
			nodeIdx = i
			break
		}
	}

	if nodeIdx > 0 {
		leftSibling := parent.children[nodeIdx-1]
		if len(leftSibling.bunches) > (bt.degree-1)/2 {
			node.bunches = append([]ValPtrsBunch{parent.bunches[nodeIdx-1]}, node.bunches...)
			parent.bunches[nodeIdx-1] = leftSibling.bunches[len(leftSibling.bunches)-1]
			leftSibling.bunches = leftSibling.bunches[:len(leftSibling.bunches)-1]
			return
		}
	}

	if nodeIdx < len(parent.children)-1 {
		rightSibling := parent.children[nodeIdx+1]
		if len(rightSibling.bunches) > (bt.degree-1)/2 {
			node.bunches = append(node.bunches, parent.bunches[nodeIdx])
			parent.bunches[nodeIdx] = rightSibling.bunches[0]
			rightSibling.bunches = rightSibling.bunches[1:]
			return
		}
	}

	if nodeIdx > 0 {
		leftSibling := parent.children[nodeIdx-1]
		leftSibling.bunches = append(leftSibling.bunches, parent.bunches[nodeIdx-1])
		leftSibling.bunches = append(leftSibling.bunches, node.bunches...)
		leftSibling.children = append(leftSibling.children, node.children...)
		parent.bunches = append(parent.bunches[:nodeIdx-1], parent.bunches[nodeIdx:]...)
		parent.children = append(parent.children[:nodeIdx], parent.children[nodeIdx+1:]...)
	} else {
		rightSibling := parent.children[nodeIdx+1]
		node.bunches = append(node.bunches, parent.bunches[nodeIdx])
		node.bunches = append(node.bunches, rightSibling.bunches...)
		node.children = append(node.children, rightSibling.children...)
		parent.bunches = append(parent.bunches[:nodeIdx], parent.bunches[nodeIdx+1:]...)
		parent.children = append(parent.children[:nodeIdx+1], parent.children[nodeIdx+2:]...)
	}

	bt.rebalanceAfterDeletion(parent)
}

func (bt *BTreeIndexer) Update(oldVal, newVal cm.Blob, ptr int) error {
	if err := bt.Delete(oldVal, ptr); err != nil {
		return fmt.Errorf("failed to delete old value: %w", err)
	}

	if err := bt.Add(newVal, ptr); err != nil {
		return fmt.Errorf("failed to add new value: %w", err)
	}

	return nil
}

func (bt *BTreeIndexer) search(val cm.Blob) (*Node, int) {
	node := bt.root
	for node != nil {
		i := sort.Search(len(node.bunches), func(i int) bool {
			return bt.compareFunc(node.bunches[i].val, val) >= 0
		})
		if i < len(node.bunches) && bt.compareFunc(node.bunches[i].val, val) == 0 {
			return node, i
		}
		if node.isLeaf {
			break
		}
		node = node.children[i]
	}
	return nil, 0
}

func (bt *BTreeIndexer) searchWithBinary(val cm.Blob) (*Node, int) {
	node := bt.root
	for node != nil {
		i := sort.Search(len(node.bunches), func(i int) bool {
			return bt.compareFunc(node.bunches[i].val, val) >= 0
		})
		if i < len(node.bunches) && bt.compareFunc(node.bunches[i].val, val) == 0 {
			return node, i
		}
		if node.isLeaf {
			break
		}
		node = node.children[i]
	}
	return nil, 0
}

func (bt *BTreeIndexer) Find(val cm.Blob) []int {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	node, idx := bt.searchWithBinary(val)
	if node == nil {
		return []int{}
	}

	ptrs := node.bunches[idx].ptrs
	result := make([]int, len(ptrs))
	copy(result, ptrs)
	return result
}

func (bt *BTreeIndexer) FindInRange(min, max cm.Blob) []int {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	var result []int
	if bt.root == nil {
		return result
	}

	bt.collectInRangeFromNodeBinary(bt.root, min, max, &result)
	return result
}

func (bt *BTreeIndexer) collectInRangeFromNodeBinary(node *Node, min, max cm.Blob, result *[]int) {
	if node == nil {
		return
	}

	start := sort.Search(len(node.bunches), func(i int) bool {
		return !cm.Less(node.bunches[i].val, min, bt.compareFunc)
	})

	for i := start; i < len(node.bunches); i++ {
		if cm.Greater(node.bunches[i].val, max, bt.compareFunc) {
			break
		}

		*result = append(*result, node.bunches[i].ptrs...)

		if !node.isLeaf {
			bt.collectInRangeFromNodeBinary(node.children[i], min, max, result)
		}
	}

	if !node.isLeaf {
		bt.collectInRangeFromNodeBinary(node.children[len(node.bunches)], min, max, result)
	}
}

func (bt *BTreeIndexer) PrintHorizontal() {
	if bt.root == nil {
		fmt.Println("(empty tree)")
		return
	}
	printHorizontalNode(bt.root, "", true)
}

func printHorizontalNode(node *Node, prefix string, isLast bool) {
	fmt.Print(prefix)
	if isLast {
		fmt.Print("└── ")
		prefix += "    "
	} else {
		fmt.Print("├── ")
		prefix += "│   "
	}

	fmt.Printf("%v\n", node.bunches)

	for i, child := range node.children {
		printHorizontalNode(child, prefix, i == len(node.children)-1)
	}
}
