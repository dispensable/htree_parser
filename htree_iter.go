//go:build exclude

package store

import (
	"fmt"
)

/*
   this file is a patch to gobeansdb
   you should cp this file to gobeansdb/sotre/
   then use make build to build htree parser
*/

type NodeIterF func(*Node)

type KeyHashIterLimiter struct {
	enable bool
	
	start int
	limit int

	startFrom *int
	hasCollected *int
}

func NewKeyHashIterLimiter(enable bool, start, limit int) *KeyHashIterLimiter {
	limiter := new(KeyHashIterLimiter)
	startFrom := start
	hasCollected := 0

	limiter.enable = enable
	limiter.start = start
	limiter.limit = limit

	limiter.startFrom = &startFrom
	limiter.hasCollected = &hasCollected

	return limiter
}

func (l *KeyHashIterLimiter) AddCollected(collected int) int {
	*l.hasCollected += collected
	return *l.hasCollected
}

func (l *KeyHashIterLimiter) UpdateStartFrom(hasSkipped int) int {
	*l.startFrom -= hasSkipped
	return *l.startFrom
}

func (l *KeyHashIterLimiter) ShouldStopIter() bool {
	return l.limit > 0 && *l.hasCollected >= l.limit
}

func (l *KeyHashIterLimiter) HasLimit() bool {
	return l.enable && l.limit > 0
}

func (l *KeyHashIterLimiter) GetStartFrom() int {
	return *l.startFrom
}

func NewHTree(depth, bucketID, height int) *HTree {
	return newHTree(depth, bucketID, height)
}

func (tree *HTree) SetBucketID(bucketID int) {
	tree.bucketID = bucketID
}

func (tree *HTree) Load(path string) (err error) {
	return tree.load(path)
}

func (tree *HTree) collectItemsWithFunc(
	ni *NodeInfo, f ItemFunc, iterLimiter *KeyHashIterLimiter,
	filterkeyhash, filtermask uint64) {
	if ni.level >= len(tree.levels)-1 { // leaf
		if !iterLimiter.enable {
			tree.leafs[ni.offset].Iter(f, ni)
		} else {
			if iterLimiter.ShouldStopIter() {
				return
			} else {
				tree.leafs[ni.offset].IterWithStartLimit(f, ni, iterLimiter)
			}
		}
	} else {
		var c NodeInfo
		c.level = ni.level + 1
		var cpathBuf [8]int
		c.path = cpathBuf[:tree.depth+c.level]
		copy(c.path, ni.path)
		for i := 0; i < 16; i++ {
			c.offset = ni.offset*16 + i
			c.node = &tree.levels[c.level][c.offset]
			c.path[tree.depth+ni.level] = i
			tree.collectItemsWithFunc(&c, f, iterLimiter, filterkeyhash, filtermask)
		}
	}
}

func (tree *HTree) IterKeyHashes(f ItemFunc, n NodeIterF, start int, limit int) error {
	logger.Infof("tree node info: %v", tree.ni)

	// copy from list top
	pathFormater := fmt.Sprintf("%%0%dx", tree.depth)
	path := fmt.Sprintf(pathFormater, tree.bucketID)
	logger.Infof("== path: %s", path)
	ki := &KeyInfo{
		StringKey: path,
		Key:       []byte(path),
		KeyIsPath: true}
	ki.Prepare()
	ki.KeyPos.BucketID = tree.bucketID
	logger.Infof("=== ki: %+v", ki)
	logger.Infof("=== tree: depth: %d bbucket id: %d height: %d", tree.depth, tree.bucketID, len(tree.levels))

	// copy from listDir
	if len(ki.Key) < tree.depth {
		return fmt.Errorf("bad dir path to list: too short")
	}
	tree.Lock()
	defer tree.Unlock()

	var ni NodeInfo
	if len(ki.KeyPath) == tree.depth {
		ni.node = &tree.levels[0][0]
		ni.path = ki.KeyPath
	} else {
		tree.getNode(ki, &ni)
	}
	node := ni.node
	tree.updateNodes(ni.level, ni.offset)

	logger.Infof("node count: %+v", node)
	logger.Infof("ni: %+v", ni)

	il := NewKeyHashIterLimiter(true, start, limit)
	if ni.level >= len(tree.levels)-1 || node.count > 0 {
		if n != nil {
			n(node)
		}
		var filtermask uint64 = 0xffffffffffffffff
		shift := uint(64 - len(ki.StringKey)*4)
		filtermask = (filtermask >> shift) << shift
		tree.collectItemsWithFunc(&ni, f, il, ki.KeyHash, filtermask)
	} else {
		nodes := make([]*Node, 16)
		for i := 0; i < 16; i++ {
			nodes[i] = &tree.levels[ni.level+1][ni.offset*16+i]
		}

		if n != nil {
			for _, nodeItem := range nodes {
				n(nodeItem)
			}
		}
	}
	return nil
}

func (sh *SliceHeader) IterWithStartLimit(f ItemFunc, ni *NodeInfo, iL *KeyHashIterLimiter) (*KeyHashIterLimiter) {
	leaf := sh.ToBytes()
	lenKHash := Conf.TreeKeyHashLen
	lenItem := lenKHash + TREE_ITEM_HEAD_SIZE
	mask := Conf.TreeKeyHashMask

	nodeKHash := uint64(getNodeKhash(ni.path)) << 32 & (^Conf.TreeKeyHashMask)
	var m HTreeItem
	var khash uint64
	size := len(leaf)

	start := iL.GetStartFrom()
	if start == 0 && !iL.HasLimit() {
		for i := 0; i < size; i += lenItem {
			bytesToItem(leaf[i+lenKHash:], &m)
			khash = bytesToKhash(leaf[i:])
			khash &= mask
			khash |= nodeKHash
			f(khash, &m)
		}
		iL.AddCollected(size / lenItem)
	} else if start > 0 && iL.limit == 0 {
		if size / lenItem < start {
			iL.UpdateStartFrom(size /lenItem)
			return iL
		}

		iL.UpdateStartFrom(start)
		for i := start * lenItem; i < size; i += lenItem {
			bytesToItem(leaf[i+lenKHash:], &m)
			khash = bytesToKhash(leaf[i:])
			khash &= mask
			khash |= nodeKHash
			f(khash, &m)
		}
	} else {
		if size / lenItem < start {
			iL.UpdateStartFrom(size/lenItem)
			return iL
		}

		iL.UpdateStartFrom(start)
		for i := start*lenItem; i < size; i += lenItem {
			bytesToItem(leaf[i+lenKHash:], &m)
			khash = bytesToKhash(leaf[i:])
			khash &= mask
			khash |= nodeKHash
			f(khash, &m)
			iL.AddCollected(1)
			// logger.Infof("collected: %d", *iL.hasCollected)
			if iL.ShouldStopIter() {
				break
			}
		}
	}
	return iL
}

func GetNodeCount(n *Node) uint32 {
	return n.count
}
