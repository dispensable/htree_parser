package dumper

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/douban/gobeansdb/store"
)

type KeyDumpType int

const (
	HashKey KeyDumpType = 0
	StrKey KeyDumpType = 1
	NurlKey KeyDumpType = 2
)

type KeyDumper struct {
	BktNumers int
	HtreeHeight int
	DBhashFile string
	StarFrom int
	Limit int

	// keypattern from cli
	KeyPatternRaw string
	KeyPatternRegex *regexp.Regexp
	KeyPatternRegexes []*regexp.Regexp
	NotKeyPatternRegexes []*regexp.Regexp
	
	DBAddr string
	DBPort int
	BktPath string
	BktPathArray []string
	DumpFMgr *DumpFileMgr
	SleepInterval int
	Progress int
	KeyType KeyDumpType

	HTree *store.HTree
	KeyFinder *KeyFinder

	cfg *DumperCfg
}

func NewKeyDumper(
	bktNum, htreeHeight, start, limit, dbPort, maxFileSizeMB, sleepInterval, progress *int,
	hashF, bktPath, keyPattern, dbAddr, dumpToDir, logLevel, cfgFile *string,
	keyType KeyDumpType,
) (*KeyDumper, error) {
	keyDumper := new(KeyDumper)
	keyDumper.KeyType = keyType
	keyDumper.BktNumers = *bktNum
	keyDumper.HtreeHeight = *htreeHeight
	keyDumper.DBhashFile = *hashF
	keyDumper.StarFrom = *start
	keyDumper.Limit = *limit
	keyDumper.KeyPatternRaw = *keyPattern
	if *keyPattern != "" {
		if keyType == HashKey {
			return nil, fmt.Errorf("hash key type do not support -k option")
		}
		re, err := regexp.Compile(keyDumper.KeyPatternRaw)
		if err != nil {
			return nil, err
		}
		keyDumper.KeyPatternRegex = re
		keyDumper.KeyPatternRegexes = append(keyDumper.KeyPatternRegexes, re)
	}

	keyDumper.DBAddr = *dbAddr
	keyDumper.DBPort = *dbPort

	mgr, err := NewDumpFileMgr(bktPath, dumpToDir, logLevel, maxFileSizeMB, keyType)
	if err != nil {
		return nil, err
	}

	keyDumper.DumpFMgr = mgr
	keyDumper.SleepInterval = *sleepInterval
	keyDumper.Progress = *progress
	keyDumper.BktPath = *bktPath
	keyDumper.BktPathArray = strings.Split(keyDumper.BktPath, "/")

	if *cfgFile != "" {
		cfg, err := NewDumperCfgFromFile(*cfgFile)
		if err != nil {
			return nil, err
		}
		keyDumper.cfg = cfg

		for _, p := range keyDumper.cfg.Dumper.KeyPatterns {
			keyDumper.KeyPatternRegexes = append(
				keyDumper.KeyPatternRegexes,
				regexp.MustCompile(p),
			)
		}

		for _, np := range keyDumper.cfg.Dumper.NotKeyPatterns {
			keyDumper.NotKeyPatternRegexes = append(
				keyDumper.NotKeyPatternRegexes,
				regexp.MustCompile(np),
			)
		}
	}
	log.Debugf("keyDumper: %+v", keyDumper)
	return keyDumper, nil
}

func (k *KeyDumper) loadHtree() (*store.HTree, error) {
	// set conf for htree parser
	n := k.BktNumers
	c := store.Conf
	c.TreeDepth = 0
	c.TreeHeight = k.HtreeHeight
	for n > 1 {
		c.TreeDepth += 1
		n /= 16
	}
	// TreeKeyHashLen & TreeKeyHashMask
	c.TreeKeyHashLen = store.KHASH_LENS[c.TreeDepth+c.TreeHeight-1]
	shift := 64 - uint32(c.TreeKeyHashLen)*8
	c.TreeKeyHashMask = (uint64(0xffffffffffffffff) << shift) >> shift

	// parse hash file to scan
	bucketID := 0
	for idx, b := range k.BktPathArray {
		if i, err := strconv.ParseInt(b, 16, 64); err != nil {
			return nil, fmt.Errorf("parse db path err: %v", err)
		} else {
			bucketID += int(math.Pow(16, float64(len(k.BktPathArray) - (idx + 1)))) * int(i)
		}
	}
	log.Infof("bucket id: %v\n", bucketID)
	ht := store.NewHTree(c.TreeDepth, bucketID, k.HtreeHeight)
	ht.SetBucketID(bucketID)
	ht.Load(k.DBhashFile)
	return ht, nil
}

func (k *KeyDumper) getHashKeyItemF(nodeTotal *uint64, procceeded *uint64) store.ItemFunc {
	progressV := uint64(k.Progress)
	sleepInter := k.SleepInterval
	dLogger := k.DumpFMgr.DumpLogger

	if sleepInter == 0 && progressV == 0 {
		return func(khash uint64, item *store.HTreeItem) {
			log.Debugf("hash key: %x -> %v\n", khash, item.Pos)
			dLogger.Printf("%016x", khash)
		}
	} else if sleepInter == 0 {
		return func(khash uint64, item *store.HTreeItem) {
			log.Debugf("hash key: %x -> %v\n", khash, item.Pos)
			dLogger.Printf("%016x", khash)

			if progressV > 0 {
				*procceeded += 1
				if *procceeded % progressV == 0 {
					log.Infof("dumped key procceeded: %d, total: %d", *procceeded, *nodeTotal)
				}
			}
		}
	} else {
		return func(khash uint64, item *store.HTreeItem) {
			log.Debugf("hash key: %x -> %v\n", khash, item.Pos)
			dLogger.Printf("%016x", khash)

			time.Sleep(time.Millisecond * time.Duration(sleepInter))

			if progressV != 0 {
				*procceeded += 1
				if *procceeded % progressV == 0 {
					log.Infof("dumped key procceeded: %d, total: %d", *procceeded, *nodeTotal)
				}
			}
		}
	}
}

func (k *KeyDumper) getStrKeyItemF(nodeTotal *uint64, procceeded *uint64, keyFinder *KeyFinder) store.ItemFunc {
	progressV := uint64(k.Progress)
	sleepInter := k.SleepInterval
	dLogger := k.DumpFMgr.DumpLogger
	needKeyStrMatch := len(k.KeyPatternRegexes) != 0 || len(k.NotKeyPatternRegexes) != 0
	res := k.KeyPatternRegexes
	nres := k.NotKeyPatternRegexes

	return func(khash uint64, item *store.HTreeItem) {
		log.Debugf("hash key: %x -> %v\n", khash, item.Pos)
		keyBytes, err := keyFinder.GetKeyByHash(khash)
		if err != nil {
			log.Errorf("got key err: %s", err)
		} else {
			keyStr := string(keyBytes)
			log.Debugf(">>> got key: %s", keyStr)
			if needKeyStrMatch {
				canSelect := true
				for _, nre := range nres {
					if nre.MatchString(keyStr) {
						canSelect = false
						break
					}
				}

				if canSelect {
					for _, re := range res {
						if re.MatchString(keyStr) {
							dLogger.Infof(keyStr)
							break
						}
					}
				}
			} else {
				dLogger.Info(keyStr)
			}
		}

		if sleepInter > 0 {
			time.Sleep(time.Millisecond * time.Duration(sleepInter))
		}

		if progressV > 0 {
			*procceeded += 1
			if *procceeded % progressV == 0 {
				log.Infof("dumped key procceeded: %d, total: %d", *procceeded, *nodeTotal)
			}
		}
	}
} 

func (k *KeyDumper) DumpKeys() error {
	// iter htree
	log.Infof("loading htree from %s ...", k.DBhashFile)
	ht, err := k.loadHtree()
	if err != nil {
		return err
	}

	// iter htree
	log.Infof("dumping keyfile to: %s, waiting ...", k.DumpFMgr.DumpFile)

	nodeTotal := uint64(0)
	procceeded := uint64(0)

	var itemF store.ItemFunc
	var ktStr string
	switch k.KeyType {
	case HashKey:
		ktStr = "hash"
		itemF = k.getHashKeyItemF(&nodeTotal, &procceeded)
	case StrKey:
		ktStr = "str"
		keyFinder, err := NewKeyFinder(k.DBAddr, uint16(k.DBPort))
		if err != nil {
			return fmt.Errorf("create str key finder err: %v", err)
		}
		defer keyFinder.Close()
		itemF = k.getStrKeyItemF(&nodeTotal, &procceeded, keyFinder)
	default:
		return fmt.Errorf("key type not support: %v", k.KeyType)
	}

	nodeF := func(node *store.Node) {
		log.Infof(">> node: %+v\n", node)
		nodeTotal += uint64(store.GetNodeCount(node))
	}

	ht.IterKeyHashes(itemF, nodeF, k.StarFrom, k.Limit)
	log.Infof("dump key (type: %s) success, file: %s | dumped total: %d", ktStr, k.DumpFMgr.DumpFile, procceeded)
	return nil
}


