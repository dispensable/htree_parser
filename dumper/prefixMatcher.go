package dumper

import (
	"fmt"

	"github.com/viant/ptrie"
)


type PrefixMatcher struct {
	trie *ptrie.Trie
	defaultT int
}


func NewPrefixMatcher(prefixes, notPrefixes []string, defaultV int) (*PrefixMatcher, error) {
	t := ptrie.New()

	for _, ap := range prefixes {
		err := t.Put([]byte(ap), PrefixAllowDump)
		if err != nil {
			return nil, fmt.Errorf("put prefix %s to trie err: %s", ap, err)
		}
	}

	for _, np := range notPrefixes {
		err := t.Put([]byte(np), PrefixSkipDump)
		if err != nil {
			return nil, fmt.Errorf("put not allow prefix %s to trie err: %s", np, err)
		}
	}

	result := &PrefixMatcher{
		trie: &t,
		defaultT: defaultV,
	}
	
	return result, nil
}

func (p *PrefixMatcher) GetV(key []byte) int {
	var result int
	v := (*(p.trie)).MatchPrefix(key, func(key []byte, value interface{}) bool {
		result = value.(int)
		return true
	})

	if !v {
		return p.defaultT
	} else {
		return result
	}
}
