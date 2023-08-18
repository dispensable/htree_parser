package dumper

import (

	"github.com/acomagu/trie/v2"
)


type PrefixMatcher struct {
	trie *trie.Tree[byte, int]
	defaultT int
}


func NewPrefixMatcher(prefixes, notPrefixes []string, defaultV int) (*PrefixMatcher, error) {
	kBytes := [][]byte{}
	vInts := []int{}

	for _, ap := range prefixes {
		kBytes = append(kBytes, []byte(ap))
		vInts = append(vInts, PrefixAllowDump)
	}

	for _, np := range notPrefixes {
		kBytes = append(kBytes, []byte(np))
		vInts = append(vInts, PrefixSkipDump)
	}

	t := trie.New[byte, int](kBytes, vInts)

	result := &PrefixMatcher{
		trie: &t,
		defaultT: defaultV,
	}

	return result, nil
}

func (p *PrefixMatcher) GetV(key []byte) int {
	var v int
	var match bool

	n := *(p.trie)

	for _, c := range key {
		if n = n.TraceOne(c); n == nil {
			break
		}

		if vv, ok := n.Terminal(); ok {
			v = vv
			match = true
		}
	}

	if match {
		return v
	} else {
		return p.defaultT
	}
}
