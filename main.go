package main

import (
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/douban/gobeansdb/store"
	logrus "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
	rotateLogger "gopkg.in/natefinch/lumberjack.v2"
)

var (
	log = logrus.New()
	dumpLogger = logrus.New()
)

const (
	dumpFilePatternFormat = "dump_key_btk_%s.keyfile"
)

type DumpKeyFormatter struct {
	logrus.TextFormatter
}

func (f *DumpKeyFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	return []byte(fmt.Sprintf("%s\n", entry.Message)), nil
}

func setLogLevel(logLevel string) {
	l, err := logrus.ParseLevel(logLevel)
	if err != nil {
		log.Warnf("log level no supported will use info level (passed %s)", logLevel)
	}
	log.SetLevel(l)
	log.SetFormatter(&logrus.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})
}

func main() {
	var BtkNumbers *int = flag.IntP("btk-bumber", "b", 256, "number of beansdbs bucket (/etc/gobeansdb/route.yaml)")
	var HtreeHeight *int = flag.IntP("htree-height", "H", 6, "height of htree: /etc/gobeansdb/global.yaml -> htree_height")
	var DBHashFile *string = flag.StringP("hash-file", "f", "/var/lib/beansdb/x/x/*.hash", "hash file")
	var dbpathRaw *string = flag.StringP("db-path", "p", "", "db bucket path, eg a/b bucket")
	var keyStart *int = flag.IntP("start", "s", 0, "key hash cnt start from")
	var keyLimit *int = flag.IntP("limit", "l", 100, "key hash cnt limit")
	var dbAddr *string = flag.StringP("db-addr", "d", "127.0.0.1", "beansdb addr")
	var dbPort *int = flag.IntP("db-port", "P", 7900, "beansdb port")
	var dumpTo *string = flag.StringP("dump-to-dir", "D", "./", "dump to dir")
	var rotateSize *int = flag.IntP("max-file-size-mb", "S", 500, "rotate file when dump file size over this throshold, MB")
	var sleepInterval *int = flag.IntP("sleep-interval-ms", "i", 1000, "sleep N ms during each key get")
	var loggerLevel *string = flag.StringP("log-level", "L", "info", "log level: info warn error fatal debug trace")
	var progress *int = flag.IntP("progress", "g", 1000, "show progress every N lines, 0 means no progress")
	
	flag.Parse()

	setLogLevel(*loggerLevel)
	log.Printf("btkn: %v", BtkNumbers)
	dbpath := strings.Split(*dbpathRaw, "/")
	log.Printf("dbpath : %v | %v\n", dbpathRaw, dbpath)

	// check if target folder already has file
	patternPath := []string{}
	for _, _ = range dbpath {
		patternPath = append(patternPath, "*")
	}
	dumpTargetPattern := filepath.Join(
		*dumpTo,
		fmt.Sprintf(
			dumpFilePatternFormat,
			filepath.Join(patternPath...),
		),
	)
	log.Infof("checking target file path: %s", dumpTargetPattern)
	matches, err := filepath.Glob(dumpTargetPattern)
	if err != nil {
		log.Fatalf("glob %s err: %s", dumpTargetPattern, err)
		return
	}
	if len(matches) > 0 {
		log.Fatalf("there is dumped faile exists already, clean it then retry or change a new folder: %v", matches)
		return
	}

	// set dump Logger
	dumpLogger.SetFormatter(&DumpKeyFormatter{logrus.TextFormatter{
		DisableQuote: true,
		DisableColors: true,
		DisableSorting: true,
		DisableTimestamp: true,
		DisableLevelTruncation: true,
	}})
	dumpFile := filepath.Join(*dumpTo, fmt.Sprintf(dumpFilePatternFormat, *dbpathRaw))
	dumpLogger.SetOutput(&rotateLogger.Logger{
		Filename: dumpFile,
		MaxSize: *rotateSize,
	})

	// set conf for htree parser
	n := *BtkNumbers
	c := store.Conf
	c.TreeDepth = 0
	c.TreeHeight = *HtreeHeight
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
	for idx, b := range dbpath {
		if i, err := strconv.ParseInt(b, 16, 64); err != nil {
			log.Fatalf("parse db path err: %v", err)
			return
		} else {
			bucketID += int(math.Pow(16, float64(len(dbpath) - (idx + 1)))) * int(i)
		}
	}
	log.Printf("bucket id: %v\n", bucketID)
	ht := store.NewHTree(c.TreeDepth, bucketID, *HtreeHeight)
	ht.SetBucketID(bucketID)
	ht.Load(*DBHashFile)

	keyFinder, err := NewKeyFinder(*dbAddr, uint16(*dbPort))
	if err != nil {
		log.Fatalf("new key finder err: %v", err)
		return
	}
	defer keyFinder.Close()
	
	// iter htree
	log.Infof("dumping keyfile to: %s, waiting ...", dumpFile)
	procceeded := uint64(0)
	progressV := uint64(*progress)
	nodeTotal := uint64(0)

	itemF := func(khash uint64, item *store.HTreeItem) {
		log.Debugf("hash key: %x -> %v\n", khash, item.Pos)
		keyBytes, err := keyFinder.GetKeyByHash(khash)
		if err != nil {
			log.Errorf("got key err: %s", err)
		} else {
			log.Debugf(">>> got key: %s", string(keyBytes))
			dumpLogger.Info(string(keyBytes))
		}
		if *sleepInterval > 0 {
			time.Sleep(time.Millisecond * time.Duration(*sleepInterval))
		}

		if progressV > 0 {
			procceeded += 1
			if procceeded % progressV == 0 {
				log.Infof("dumped key procceeded: %d, total: %d", procceeded, nodeTotal)
			}
		}
	}

	nodeF := func(node *store.Node) {
		log.Infof(">> node: %+v\n", node)
		nodeTotal += uint64(store.GetNodeCount(node))
	}

	ht.IterKeyHashes(itemF, nodeF, *keyStart, *keyLimit)
	log.Infof("dump key success, file: %s | dumped total: %d", dumpFile, procceeded)
}
