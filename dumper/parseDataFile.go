package dumper

import (
	"fmt"
	"hash/fnv"
	"os"
	"regexp"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/douban/gobeansdb/store"
	"github.com/douban/gobeansproxy/cassandra"
	golibmc "github.com/douban/libmc/src"
	logrus "github.com/sirupsen/logrus"
)

const (
	PrefixAllowDump int = 1
	PrefixSkipDump int = 2
)

type DataFileParser struct {
	DBDataFile string
	KeyLimit int

	KeyPatternRaw string
	KeyPatternRegex *regexp.Regexp
	KeyPatternRegexes []*regexp.Regexp
	NotKeyPatternRegexes []*regexp.Regexp

	// trie for prefix match key
	// efficient fast than regex when only prefix match
	prefixMatcher *PrefixMatcher

	SleepInterval int
	Progress int
	cfg *DumperCfg

	reader *store.DataStreamReader
	setter *KeyFinder
	writeToCstar bool
	cstarStore *cassandra.CassandraStore

	// for log mgr
	dumpFMgr *DumpFileMgr

	workerNumber int

	prefix string
	// output
	outputFunc func (p *DataFileParser, rec *store.Record, keyOnly bool) error
	rivenOutputF func (p *DataFileParser, rec *Record) error

	isRivendb bool
	KeyMgr *DumpFileMgr

	dumpType KeyDumpType
}

func NewDataFileParser(
	dfile, keyPattern, cfgFile, dbAddr, dbPathRaw, dumpTo, loggerLevel, prefix *string,
	keylimit, sleepInterval, progress, rotateSize, workerNum *int,
	dumpType KeyDumpType, dbPort *uint16,
	writeToCstar, isRivendb *bool,
) (*DataFileParser, error) {
	p := new(DataFileParser)
	p.writeToCstar = *writeToCstar
	p.DBDataFile = *dfile
	p.KeyLimit = *keylimit
	p.KeyPatternRaw = *keyPattern
	p.workerNumber = *workerNum
	p.prefix = *prefix
	p.isRivendb = *isRivendb
	p.dumpType = dumpType

	if *keyPattern != "" {
		re, err := regexp.Compile(p.KeyPatternRaw)
		if err != nil {
			return nil, err
		}
		p.KeyPatternRegex = re
		p.KeyPatternRegexes = append(p.KeyPatternRegexes, re)
	}

	p.SleepInterval = *sleepInterval
	p.Progress = *progress

	if *cfgFile != "" {
		cfg, err := NewDumperCfgFromFile(*cfgFile)
		if err != nil {
			return nil, err
		}
		p.cfg = cfg

		for _, t := range p.cfg.Dumper.KeyPatterns {
			p.KeyPatternRegexes = append(
				p.KeyPatternRegexes,
				regexp.MustCompile(t),
			)
		}

		for _, np := range p.cfg.Dumper.NotKeyPatterns {
			p.NotKeyPatternRegexes = append(
				p.NotKeyPatternRegexes,
				regexp.MustCompile(np),
			)
		}

		if len(cfg.ParseDataFile.Prefixes) > 0 || len(cfg.ParseDataFile.NotPrefixes) > 0 {
			m, err := NewPrefixMatcher(cfg.ParseDataFile.Prefixes, cfg.ParseDataFile.NotPrefixes, PrefixSkipDump)
			if err != nil {
				return nil, err
			}
			p.prefixMatcher = m
		}
	} else {
		if p.writeToCstar {
			return nil, fmt.Errorf("If set write to Cstar, you must pass cfgfile")
		}
	}

	client, err := NewKeyFinder(*dbAddr, *dbPort, 3)
	if err != nil {
		log.Errorf("Create client to %s:%d err: %s", *dbAddr, *dbPort, err)
		return nil, err
	}

	if p.isRivendb {
		p.rivenOutputF = WriteRivenDataToCstar
	}

	p.setter = client

	if p.cfg.ParseDataFile.CassandraCfg.WriteEnable {
		s, err := cassandra.NewCassandraStore(&p.cfg.ParseDataFile.CassandraCfg)
		if err != nil {
			return nil, err
		}
		p.cstarStore = s
		p.outputFunc = WriteToCstar
	} else {
		if p.writeToCstar {
			return nil, fmt.Errorf("write to cstar need cstar write cfg enabled")
		}
		p.outputFunc = WriteToDB
	}

	if p.dumpType == ParseStrKeyToF {
		m, err := NewDumpFileMgr(dbPathRaw, dumpTo, loggerLevel, rotateSize, ParseStrKeyToF, logrus.New())
		if err != nil {
			return nil, err
		}

		log.Infof("Will dump to file: %s", m.DumpFile)
		p.KeyMgr = m
	}

	if *dumpTo != "" {
		mgr, err := NewDumpFileMgr(dbPathRaw, dumpTo, loggerLevel, rotateSize, ErrorKey, nil)
		if err != nil {
			return nil, err
		}
		p.dumpFMgr = mgr
	}
	return p, nil
}

func WriteRivenDataToCstar(p *DataFileParser, rec *Record) error {
	return p.writeRivenDataToCstar(rec)
}

func (p *DataFileParser) writeRivenDataToCstar(rec *Record) error {
	if rec.Version < 0 {
		log.Warnf("Find delete key: %s", rec.Key)
		ok, err := p.cstarStore.Delete(rec.Key)
		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("delete key %s not ok", rec.Key)
		}
		return nil
	}

	value := new(cassandra.BDBValue)
	value.ReceiveTime = time.Unix(int64(rec.Timestamp), 0)
	value.Flag = int(rec.Flag)
	value.Exptime = 0
	value.Body = rec.Value

	_, err := p.cstarStore.SetWithValue(fmt.Sprintf("%s%s", p.prefix, rec.Key), value)
	if err != nil {
		log.Warnf("set key err: %s", err)
	}
	return err
}

func WriteToCstar(p *DataFileParser, rec *store.Record, keyOnly bool) error {
	return p.WriteToCstar(rec, keyOnly)
}

func (p *DataFileParser) WriteToCstar(rec *store.Record, keyOnly bool) error {
	if rec.Payload.Ver < 0 {
		log.Warnf("Find delete key: %s", rec.Key)
		ok, err := p.cstarStore.Delete(string(rec.Key))
		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("delete key %s not ok", rec.Key)
		}
		return nil
	}

	value := new(cassandra.BDBValue)
	value.ReceiveTime = time.Unix(int64(rec.Payload.TS), 0)
	value.Flag = int(rec.Payload.Flag)
	value.Exptime = 0

	if !keyOnly {
		if rec.Payload.IsCompressed() {
			defer func() {
				if err := recover(); err != nil {
					log.Infof("runtime err: %v", err)
				}
			}()
			if rec.Payload.Body == nil {
				return fmt.Errorf("can be empty")
			}

			rec.Payload.Decompress()
		}
		value.Body = rec.Payload.Body
	}
	_, err := p.cstarStore.SetWithValue(fmt.Sprintf("%s%s", p.prefix, rec.Key), value)
	return err
}

func WriteToDB(p *DataFileParser, rec *store.Record, keyOnly bool) error {
	return p.WriteToDB(rec, keyOnly)
}

func (p *DataFileParser) WriteToDB(rec *store.Record, keyOnly bool) error {
	// handle delete
	if rec.Payload.Ver < 0 {
		return p.setter.client.Delete(string(rec.Key))
	}

	// insert of update
	item := golibmc.Item{}
	item.Key = fmt.Sprintf("%s%s", p.prefix, rec.Key)
	item.Flags = rec.Payload.Flag
	if keyOnly {
		item.Value = []byte{}
	} else {
		if rec.Payload.IsCompressed() {
			defer func() {
				if err := recover(); err != nil {
					log.Infof("runtime err: %v", err)
				}
			}()
			err := rec.Payload.Decompress()
			if err != nil {
				return err
			}
		}
		item.Value = rec.Payload.Body
		log.Debugf("write k: %s v: %s", item.Key, item.Value)
	}
	return p.setter.client.Set(&item)
}


func (p *DataFileParser) StartRivenConsumer(wg *sync.WaitGroup) ([]chan *Record, error) {
	var consumerChans []chan *Record
	dmpLog := p.dumpFMgr.DumpLogger

	for i := 0; i <p.workerNumber; i++ {
		wg.Add(1)

		log.Infof("Adding worker number: %d", i)
		c := make(chan *Record, 10)
		consumerChans = append(consumerChans, c)

		go func(parser *DataFileParser, taskChan chan *Record, idx int) {
			defer wg.Done()
			log.Infof("consumer %d started ...", idx)
			total := 0
			errorCnt := 0
			for t := range taskChan {
				total += 1
				err := p.rivenOutputF(parser, t)
				if err != nil {
					log.Debugf("set value failed of key %s err: %v", t.Key, err)
					if p.dumpFMgr != nil {
						dmpLog.Println(string(t.Key))
					}
					errorCnt += 1
					continue
				}

				if p.Progress != 0 {
					if total % p.Progress == 0 {
						log.Infof("riven worker %d consumed %d records, errcnt: %d", idx, total, errorCnt)
					}
				}
			}
			log.Infof("riven consumer %d exit, total: %d, error cnt: %d", idx, total, errorCnt)
		}(p, c, i)
	}
	return consumerChans, nil
}


func (p *DataFileParser) StartConsumer(wg *sync.WaitGroup) ([]chan *store.Record, error) {
	var consumerChans []chan *store.Record

	usePrefixMatch := p.prefixMatcher != nil
	dmpLog := p.dumpFMgr.DumpLogger
	onlyWriteKeyToDB := p.dumpType == ParseStrKeyToDB

	for i := 0; i <p.workerNumber; i++ {
		wg.Add(1)

		log.Infof("Adding worker number: %d", i)
		c := make(chan *store.Record, 10)
		consumerChans = append(consumerChans, c)
		var pMatcher *PrefixMatcher
		if usePrefixMatch {
			m, err := NewPrefixMatcher(p.cfg.ParseDataFile.Prefixes, p.cfg.ParseDataFile.NotPrefixes, PrefixSkipDump)
			if err != nil {
				return nil, err
			}
			pMatcher = m
		}

		go func(parser *DataFileParser, taskChan chan *store.Record, idx int, pMatcher *PrefixMatcher) {
			defer wg.Done()
			log.Infof("consumer %d started ...", idx)
			total := 0
			errorCnt := 0
			skipped := 0
			for t := range taskChan {
				total += 1

				err := p.outputFunc(parser, t, onlyWriteKeyToDB)
				if err != nil {
					log.Debugf("set value failed of key %s err: %v", t.Key, err)
					if p.dumpFMgr != nil {
						dmpLog.Println(string(t.Key))
					}
					errorCnt += 1
					continue
				}

				if p.Progress != 0 {
					if total % p.Progress == 0 {
						log.Infof("worker %d consumed %d records, errcnt: %d, skipped: %d", idx, total, errorCnt, skipped)
					}
				}
			}
			log.Infof("consumer %d exit, total: %d, error cnt: %d, skipped: %d", idx, total, errorCnt, skipped)
		}(p, c, i, pMatcher)
	}
	return consumerChans, nil
}

func hash(s []byte) uint32 {
	h := fnv.New32a()
	h.Write(s)
	return h.Sum32()
}

func (p *DataFileParser) ParseRiven() error {
	f, err := os.Open(p.DBDataFile)
	if err != nil {
		return fmt.Errorf("open file %s err: %s", p.DBDataFile, err)
	}
	defer f.Close()

	var wg sync.WaitGroup
	recChan, err := p.StartRivenConsumer(&wg)
	if err != nil {
		return err
	}
	blen := uint32(len(recChan))

	decompressErrCnt := 0
	cnt := 0
	dumpKeyToF := p.dumpType == ParseStrKeyToF

	for {
		rec, err := ReadRecordNext(f)
		if err != nil {
			return fmt.Errorf("read record of rivedb %s err: %s", p.DBDataFile, err)
		}

		if rec == nil {
			log.Infof("EOF of %s", p.DBDataFile)
			break
		}

		if dumpKeyToF {
			p.KeyMgr.DumpLogger.Println(rec.Key)
			continue
		}

		// hash to bucket chan
		idx := hash([]byte(rec.Key)) % blen

		log.Debugf("send rec: %+v", rec)
		recChan[idx] <- rec
		cnt += 1
		if p.Progress != 0 && cnt % p.Progress == 0 {
			log.Infof("produced tasks: %d, decompress failed: %d", cnt, decompressErrCnt)
		}

		if p.KeyLimit != 0 && cnt >= p.KeyLimit {
			log.Infof("produced tasks matched user limit: %d", cnt)
			break
		}

		if p.SleepInterval != 0 {
			time.Sleep(time.Duration(p.SleepInterval) * time.Millisecond)
		}
	}

	for {
		finished := true
		for _, c := range recChan {
			if len(c) != 0 {
				finished = false
				break
			}
		}
		if finished {
			for cidx, c := range recChan {
				log.Infof("closing consumer %d, cause finished", cidx)
				close(c)
			}

			break
		} else {
			time.Sleep(1 * time.Second)
		}
	}

	wg.Wait()
	
	return nil
}

func (p *DataFileParser) Parse(enableProf bool) error {
	if enableProf {
		f, err := os.Create("/tmp/htree_parser.profile")
		if err != nil {
			return err
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	
	// init reader from gobeansdb
	reader, err := store.NewDataStreamReader(p.DBDataFile, 4096)
	if err != nil {
		return err
	}
	defer reader.Close()
	cnt := 0

	var wg sync.WaitGroup
	recChan, err := p.StartConsumer(&wg)
	if err != nil {
		return err
	}
	blen := uint32(len(recChan))

	decompressErrCnt := 0

	usePrefixMatch := p.prefixMatcher != nil
	onlyWriteKeyToF := p.dumpType == ParseStrKeyToF
	var keyDumpLogger *logrus.Logger
	if onlyWriteKeyToF {
		keyDumpLogger = p.KeyMgr.DumpLogger
	}

	var pMatcher *PrefixMatcher
	if usePrefixMatch {
		m, err := NewPrefixMatcher(p.cfg.ParseDataFile.Prefixes, p.cfg.ParseDataFile.NotPrefixes, PrefixSkipDump)
		if err != nil {
			return err
		}
		pMatcher = m
	}
	total := 0
	skipped := 0

	for {
		rec, offset, _, err := reader.Next()
		if err != nil {
			log.Errorf("get record at offset: %d err: %s", offset, err)
		}

		if rec == nil {
			log.Infof("File %s parse end, total cnt: %d", p.DBDataFile, cnt)
			break
		}

		total += 1
		if usePrefixMatch {
			// handle prefix match logic
			v := pMatcher.GetV(rec.Key)
			if v == PrefixSkipDump {
				skipped += 1
				continue
			}
		}

		if onlyWriteKeyToF {
			log.Debugf("will dump key to logger: %s", p.KeyMgr.DumpFile)
			keyDumpLogger.Println(fmt.Sprintf("%s, %d", rec.Key, rec.Payload.Ver))
			continue
		}

		if rec.Payload.IsCompressed() {
			err := rec.Payload.Decompress()
			if err != nil {
				decompressErrCnt += 1
				continue
			}
		}

		// log.Debugf("--> get key: %s | v: %+v", rec.Key, rec)
		// hash to bucket chan
		idx := hash(rec.Key) % blen

		recChan[idx] <- rec
		cnt += 1
		if p.Progress != 0 && cnt % p.Progress == 0 {
			log.Infof("produced tasks: %d, decompress failed: %d", cnt, decompressErrCnt)
		}

		if p.KeyLimit != 0 && cnt >= p.KeyLimit {
			log.Infof("produced tasks matched user limit: %d", cnt)
			break
		}

		if p.SleepInterval != 0 {
			time.Sleep(time.Duration(p.SleepInterval) * time.Millisecond)
		}
	}

	for {
		finished := true
		for _, c := range recChan {
			if len(c) != 0 {
				finished = false
				break
			}
		}
		if finished {
			for cidx, c := range recChan {
				log.Infof("closing consumer %d, cause finished", cidx)
				close(c)
			}

			break
		} else {
			time.Sleep(1 * time.Second)
		}
	}

	wg.Wait()
	log.Infof("Producer produced: %d skip: %d", total, skipped)
	return nil
}
