package main

import (
	"bytes"
	"encoding/binary"
)

const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

type BNode []byte // what is used to dump into disk

type BTree struct {
	// pointer to a page number
	root uint64
	// managing pages on-disk
	get func(uint64) []byte // derefering a pointer
	new func([]byte) uint64 // allocating a new page
	del func(uint64) // deallocating a page
}

// header functions
const (
	BNODE_NODE = 1 // internal nodes (no val)
	BNODE_LEAF = 2 // leaf nodes
)

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16)  {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// pointer functions
func (node BNode) getPtr(idx uint16) uint64 {
	if idx > node.nkeys() {
		panic("index exceeds number of keys")
	}
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	if idx > node.nkeys() {
		panic("index exceeds number of keys")
	}
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

// offset list: is an array in which u can index to get the relative position
func offsetPos(node BNode, idx uint16) uint16 {
	if !(1 <= idx && idx <= node.nkeys()) {
		panic("index out of bounds")
	}
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.AppendUint16(node[offsetPos(node, idx):], offset)
}

// key-values
func (node BNode) kvPos(idx uint16) uint16 {
	if idx > node.nkeys() {
		panic("index exceeds number of keys")
	}
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	if idx >= node.nkeys() {
		panic("index exceeds or equals the number of keys")
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	if idx >= node.nkeys() {
		panic("index exceeds or equals the number of keys")
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}

// node byte size
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// child node lookup 
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)
	// first key is copy from parent node so it's <= to the key
	// we want the largest
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// adding a key to a leaf node
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys() + 1)
	nodeAppendRange(new, old, 0, 0, idx) // copies range of kv
	nodeAppendKV(new, idx, 0, key, val) // copies kv pair
	nodeAppendRange(new, old, idx + 1, idx, old.nkeys() - idx)
}

func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx + 1, idx + 1, old.nkeys() - idx - 1)
}

func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys() - 1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx + 1, idx + 1, old.nkeys() - idx - 1)
}

func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys() + right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}

func nodeReplace2Kid(new BNode, old BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(old.btype(), old.nkeys() - 1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, ptr, key, nil)
	nodeAppendRange(new, old, idx + 1, idx + 2, old.nkeys() - idx - 2)
}

// copy kv into position
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptr
	new.setPtr(idx, ptr)
	// kvs
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)
	// add offset of next key
	new.setOffset(idx + 1, new.getOffset(idx) + 4 + uint16((len(key) + len(val))))
}

// dstNew is start of new node srcOld is start of old, n is number of pairs
func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	// ptrs
	for i := uint16(0); i < n; i++ {
		new.setPtr(i + dstNew, old.getPtr(i + srcOld))
	}
	// kvs
	start := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new[new.kvPos(dstNew):], old[start:end])
	// offsets
	for i := uint16(0); i <= n; i++ { // indexes of kvs range from [1, n] as index 0 is offset 0
		// new offset would just be end of pair relative to the first pair added to the
		// offset in the new node
		new.setOffset(dstNew + i, new.getOffset(dstNew) + old.getOffset(srcOld + i) - old.getOffset(srcOld)) 
	}
}

// updating internal nodes
func nodeReplaceKidN(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1) // subtracting by 1 bc one of the new nodes is the original child node
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil) // val is nil bc its an internal node
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))

}

func nodeSplit2(left BNode, right BNode, old BNode) {
	leftSplit := old.nkeys() / 2 // 

	for (4 + 8 * leftSplit + 2 * leftSplit + old.getOffset(leftSplit)) > BTREE_PAGE_SIZE{
		leftSplit--
	}

	rightSplit := old.nkeys() - leftSplit
	// left node (guaranteed to fit)
	left.setHeader(old.btype(), leftSplit)
	nodeAppendRange(left, old, 0, 0, leftSplit)
	right.setHeader(old.btype(), rightSplit)
	nodeAppendRange(right, left, 0, leftSplit, rightSplit)
}

// returns number of split nodes and array of nodes
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old} // no split needed
	}
	left := BNode(make([]byte, BTREE_PAGE_SIZE))
	right := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // since right isnt guaranteed to fit, it may be split
	nodeSplit2(left, right, old)
	if right.nbytes() <= BTREE_PAGE_SIZE {
		right = right[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}
	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE)) // no need to double size since both should fit
	nodeSplit2(leftleft, middle, left)
	return 3, [3]BNode{leftleft, middle, right}
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))

	// find where to insert the key
	idx := nodeLookupLE(node, key)
	
	switch node.btype() {
	// if it is a leaf node, then just update it
	case BNODE_LEAF:
		// if the key is in the node, then update val
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(new, node, idx, key, val)
		} else {
			leafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		nodeInsert(tree, new, node, idx, key, val)
	}

	return new
}

func nodeInsert(tree *BTree, new BNode, node BNode, idx uint16, key []byte, val []byte) {
	kptr := node.getPtr(idx)
	knode := treeInsert(tree, tree.get(kptr), key, val)
	nsplit, split := nodeSplit3(knode)
	tree.del(kptr)
	nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
}

func (tree *BTree) Insert(key []byte, val []byte) {
	// creating first node
	if tree.root == 0 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		// we add a dummy key so lookup will always return a node
		// solves edge case: nodeLookupLE wont work if new key is less than the first key
		root.setHeader(BNODE_LEAF, 2)
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}

	node := treeInsert(tree, tree.get(tree.root), key, val)
	nsplit, split := nodeSplit3(node)
	tree.del(tree.root)
	if nsplit > 1 {
		// adding a new level bc root was split
		// root node points to the new nodes
		// where it has the 1st pair from each child
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range split[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
}

// deleting leads to empty nodes which may be merged with a sibling
// 0 indicates no, -1 indicates left, 1 indicates right
func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode){
	// not small enough so no merge
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	// check if there is a left
	if idx > 0 {
		sibling := BNode(tree.get(node.getPtr(idx - 1))) // left sibling
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling 
		}
	}
	// check if there is a right
	if idx + 1 < node.nkeys() {
		sibling := BNode(tree.get(node.getPtr(idx + 1))) // right sibling
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return 1, sibling
		}
	}

	return 0, BNode{}
}

func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{} // leaf node does not have the key
		}

		// else delete the key in the leaf
		new := BNode(make([]byte, BTREE_PAGE_SIZE))
		leafDelete(new, node, idx)
		return new
	case BNODE_NODE:
		return nodeDelete(tree, node, idx, key)
	default:
		panic("malformed node")
	}
}

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated) == 0 {
		return BNode{}
	}
	tree.del(kptr)

	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)

	switch {
	case mergeDir < 0:
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0:
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))
	case mergeDir == 0 && updated.nkeys() == 0:
		new.setHeader(BNODE_NODE, 0) // 1 empty child with no sibling, so the parent becomes empty
	case mergeDir == 0 && updated.nkeys() > 0:
		nodeReplaceKidN(tree, new, node, idx, updated) // no merge, just update
	}

	return new
}

func main() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	if node1max > BTREE_PAGE_SIZE {
		panic("node1max exceeds BTREE_PAGE_SIZE")
	}
}
