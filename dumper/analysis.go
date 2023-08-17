package dumper

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/dghubble/trie"
	"github.com/sirupsen/logrus"
)

type KeyNode struct {
	Nurl string
	Example string
	Count int
}

func NewKeyNode(nurl, key string) *KeyNode {
	return &KeyNode{
		Nurl: nurl,
		Example: key,
		Count: 1,
	}
}

func (k KeyNode) String() string {
	// return fmt.Sprintf("%s,%s,%d", k.Nurl, k.Example, k.Count)
	return fmt.Sprintf("%s %s %d", k.Nurl, k.Example, k.Count)
}

func (k *KeyNode) add() {
	k.Count += 1
}

type CounterMgr struct {
	loadFromFiles []string
	dumpFMgr *DumpFileMgr //TODO: support dump to here
	trie *trie.PathTrie
	pathRE *regexp.Regexp
	nurlREs []*regexp.Regexp
	nurlPathREs []*regexp.Regexp
	maxFieldLen int
	cfg *DumperCfg
}

func NewCounterMgr(loadFromFiles *[]string, maxFieldLen int,
	dumpTo, dbpathRaw, loggerLevel, cfgPath *string, rotateSize *int) (*CounterMgr, error) {
	r := new(CounterMgr)
	r.loadFromFiles = *loadFromFiles

	// cfg load
	cfg, err := NewDumperCfgFromFile(*cfgPath)
	if err != nil {
		return nil, err
	}
	r.cfg = cfg
	
	r.trie = trie.NewPathTrie()
	r.pathRE = regexp.MustCompile(r.cfg.Analysis.SeperatorRE)

	for _, fRE := range r.cfg.Analysis.FieldREs {
		r.nurlREs = append(r.nurlREs, regexp.MustCompile(fRE))
	}

	for _, reRaw := range r.cfg.Analysis.KeyREs {
		r.nurlPathREs = append(r.nurlPathREs,
			regexp.MustCompile(reRaw),
		)
	}

	r.maxFieldLen = maxFieldLen

	if *dumpTo == "-" {
		r.dumpFMgr = nil
	} else {
		dumpFmgr, err := NewDumpFileMgr(dbpathRaw, dumpTo, loggerLevel, rotateSize, NurlKey, nil)
		if err != nil {
			return nil, err
		}
		r.dumpFMgr = dumpFmgr
	}

	return r, nil
}

func (c *CounterMgr) trToPath(key string) string {
	return c.pathRE.ReplaceAllString(key, "/")
}

func (c *CounterMgr) nurl(path, key string) string {
	// nurl by regex
	for _, r := range c.nurlPathREs {
		// log.Infof("checking re: %s", r)
		m := r.FindStringSubmatch(key)
		if m != nil {
			for _, _m := range m[1:] {
				// log.Infof("%s match reg: %s --> %s", key, r, _m)
				key = strings.ReplaceAll(key, _m, c.cfg.Analysis.ReplacedTo)
				// log.Infof("keys replaced to: %s", key)
			}
			return c.trToPath(key)
		}
	}

	// nurl by field match
	segs := strings.Split(path, "/")
	for idx, s := range segs {
		if len(s) >= c.maxFieldLen {
			segs[idx] = c.cfg.Analysis.ReplacedTo
			continue
		}

		for _, r := range c.nurlREs {
			rms := r.FindStringSubmatch(s)
			if rms != nil {
				for _, rm := range rms[1:] {
					// log.Infof("before seg: %s", s)
					s = strings.ReplaceAll(s, rm, c.cfg.Analysis.ReplacedTo)
					// log.Infof("after replace seg: %s", s)
				}
				segs[idx] = s
				break
			}
		}
	}

	return strings.Join(segs, "/")
}

func (c *CounterMgr) add() error {
	log.Infof("start creating trie from files: %s ...", c.loadFromFiles)
	t := c.trie
	
	// read records from files
	for _, f := range c.loadFromFiles {
		log.Infof(">> processing file %s ...", f)
		file, err := os.Open(f)
		if err != nil {
			log.Errorf("open key file %s err: %s", f, err)
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			text := scanner.Text()
			path := c.trToPath(text)

			// nurl
			nurl := c.nurl(path, text)
			v := t.Get(nurl)
			if v == nil {
				newV := NewKeyNode(nurl, text)
				isNewNode := t.Put(nurl, newV)
				if !isNewNode {
					log.Errorf("Key %s abnormal, nurl: %s", text, nurl)
				}
			} else {
				keyNodeV := v.(*KeyNode)
				keyNodeV.add()
				t.Put(nurl, keyNodeV)
			}
		}
	}
	return nil
}

func (c *CounterMgr) dump() error {
	var logger *logrus.Logger
	if c.dumpFMgr == nil {
		logger = log
	} else {
		logger = c.dumpFMgr.DumpLogger
	}

	f := func(key string, v interface{}) error {
		value := v.(*KeyNode)
		logger.Println(value)
		return nil
	}
	return c.trie.Walk(f)
}

func (c *CounterMgr) Count() error {
	err := c.add()
	if err != nil {
		return err
	}

	err = c.dump()
	if err != nil {
		log.Infof("Dump end with err: %s", err)
	} else {
		log.Infof("Dump successfully")
	}
	return err
}
