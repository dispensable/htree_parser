package dumper

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type TrKeyUtils struct {
	dbAddr string
	dbPort uint16
	fromT KeyDumpType
	toT KeyDumpType
	loadF []string
	dumpFMgr *DumpFileMgr
	keyFinder *KeyFinder
	keyPatternRaw *string
	keyPatternRe *regexp.Regexp
	KeyPatternRegexes []*regexp.Regexp
	workerNum int
	progress int
	sleepInterval int
	cfg *DumperCfg
	trFunc func(hkey string, finder *KeyFinder) (string, error)
}

func NewTrKeyUtils(
	dbAddr, dumpTo, dbpathRaw, keyPattern, loggerLevel, cfgFile  *string,
	loadFromFiles *[]string,
	fromT, toT KeyDumpType,
	dbPort uint16,
	rotateSize, sleepInterval, progress, workerNum *int,
) (*TrKeyUtils, error) {

	tr := new(TrKeyUtils)
	tr.dbAddr = *dbAddr
	tr.dbPort = dbPort
	tr.fromT = fromT
	tr.toT = toT
	tr.loadF = *loadFromFiles
	tr.progress = *progress
	tr.workerNum = *workerNum
	tr.sleepInterval = *sleepInterval

	kf, err := NewKeyFinder(*dbAddr, dbPort)
	if err != nil {
		return nil, err
	}
	tr.keyFinder = kf

	mgr, err := NewDumpFileMgr(dbpathRaw, dumpTo, loggerLevel, rotateSize, toT)
	if err != nil {
		return nil, err
	}
	tr.dumpFMgr = mgr
	tr.trFunc = tr.trHkeyToStr
	
	tr.keyPatternRaw = keyPattern
	if *keyPattern != "" {
		if toT == HashKey {
			return nil, fmt.Errorf("hash key type do not support key pattern opt")
		}
		re, err := regexp.Compile(*tr.keyPatternRaw)
		if err != nil {
			return nil, err
		}
		tr.keyPatternRe = re
		tr.trFunc = tr.trHkeyToStrWhenMatch
		tr.KeyPatternRegexes = append(tr.KeyPatternRegexes, re)
	}

	if *cfgFile != "" {
		cfg, err := NewDumperCfgFromFile(*cfgFile)
		if err != nil {
			return nil, err
		}
		tr.cfg = cfg

		for _, p := range tr.cfg.TR.KeyPatterns {
			tr.KeyPatternRegexes = append(
				tr.KeyPatternRegexes,
				regexp.MustCompile(p),
			)
		}
	}
	
	return tr, nil
}

func (tr *TrKeyUtils) trHkeyToBytes(hkey string, finder *KeyFinder) ([]byte, error) {
	if finder == nil {
		finder = tr.keyFinder
	}

	hk, err := strconv.ParseUint(hkey, 16, 64)
	if err != nil {
		return nil, fmt.Errorf("parse key %v err: %v", hkey, err)
	}
	
	r, err := finder.GetKeyByHash(hk)
	if err != nil {
		return nil, fmt.Errorf("parse key %v err: %v", hkey, err)
	}

	if tr.sleepInterval != 0 {
		time.Sleep(time.Millisecond * time.Duration(tr.sleepInterval))
	}
	return r, nil
}

func (tr *TrKeyUtils) trHkeyToStrWhenMatch(hkey string, finder *KeyFinder) (string, error) {
	r, err := tr.trHkeyToStr(hkey, finder)
	if err != nil {
		log.Errorf("parse key %s failed: %s", hkey, err)
	}

	for _, re := range tr.KeyPatternRegexes {
		if re.MatchString(r) {
			return r, nil
		}
	}
	return "", fmt.Errorf("%s not match", r)
}


func (tr *TrKeyUtils) trHkeyToStr(hkey string, finder *KeyFinder) (string, error) {
	r, err := tr.trHkeyToBytes(hkey, finder)
	if err != nil {
		log.Errorf("parse key %s failed: %s", hkey, err)
	}
	return string(r), nil
}

func (tr *TrKeyUtils) TrStdinKeyFromHashKeyToStr() error {
	scanner := bufio.NewScanner(os.Stdin)
	finder := tr.keyFinder
	total := 0
	progress := tr.progress
	
	for scanner.Scan() {
		text := scanner.Text()
		if progress != 0 {
			total += 1
			if total % progress == 0 {
				log.Infof("procceeded %d records", total)
			}
		}
		r, err := tr.trFunc(text, finder)
		if err != nil {
			log.Debugf("parse `%v` to strkey err: %v", text, err)
			continue
		}
		tr.dumpFMgr.DumpLogger.Println(r)
	}
	log.Infof("procceeded %d records total", total)
	if scanner.Err() != nil {
		log.Errorf("scan end: %v", scanner.Err())
	}
	return nil
}

func (tr *TrKeyUtils) TrArgsFromHashKeyToStr(args []string) error {
	for _, hkey := range args {
		r, err := tr.trFunc(hkey, tr.keyFinder)
		if err != nil {
			log.Debugf("tr hkey `%v` to strkey err: %v", hkey, err)
			continue
		}
		tr.dumpFMgr.DumpLogger.Println(r)
	}
	return nil
}

func (tr *TrKeyUtils) TrHkeysFromFiles(files []string, workerNum int, progress int) error {
	var wg sync.WaitGroup
	var consumerChans []chan string
	
	var keyFinders []*KeyFinder
	dmpLog := tr.dumpFMgr.DumpLogger
	
	for i := 0; i <= workerNum-1; i++ {
		wg.Add(1)

		log.Infof("Adding worker number: %d", i)
		c := make(chan string, 10)
		consumerChans = append(consumerChans, c)
		kf, err := NewKeyFinder(tr.dbAddr, tr.dbPort)
		if err != nil {
			return fmt.Errorf("create keyfinder err: %v", err)
		}

		keyFinders = append(keyFinders, kf)

		go func(k *KeyFinder, taskChan chan string, idx int) {
			defer wg.Done()
			log.Infof("consumer %d started ...", idx)
			total := 0
			for t := range taskChan {
				r, err := tr.trFunc(t, kf)
				if err != nil {
					log.Debugf("get str key from hkey %v err: %v", t, err)
					continue
				}

				total += 1
				if progress != 0 {
					if total % progress == 0 {
						log.Infof("worker %d consumed %d records ...", idx, total)
					}
				}
				dmpLog.Println(r)
			}
			log.Infof("consumer %d exit, total: %d", idx, total)
		}(kf, c, i)
	}

	// producer creat tasks to worker
	log.Infof("creating producer ...")
	go func() {
		wg.Add(1)
		defer wg.Done()
		defer func() {
			for _, ch := range consumerChans {
				close(ch)
			}
		}()

		total := 0
		for _, f := range files {
			log.Infof(">> produce hkeys from file: %s", f)
			file, err := os.Open(f)
			defer file.Close()
			if err != nil {
				log.Errorf("open hkeys file %s err: %s", f, err)
				continue
			}

			scanner := bufio.NewScanner(file)
			chIdxStart := 0
			for scanner.Scan() {
				consumerChans[chIdxStart] <- scanner.Text()

				total += 1
				if progress != 0 {
					if total % progress == 0 {
						log.Infof("Add %d task records ...", total)
					}
				}

				// rotate chan for fair task load
				chIdxStart += 1
				if chIdxStart > workerNum-1 {
					chIdxStart = 0
				}
			}

			if err := scanner.Err(); err != nil {
				log.Errorf("scan file %s err: %v", f, err)
			}
		}
		log.Infof("Added %d task records total", total)
		log.Infof("all records proccessed, closing ch buffer ...")
	}()

	log.Infof("waiting for task running ...")
	wg.Wait()
	return nil
}

func (tr *TrKeyUtils) Tr(args []string) error {
	log.Debugf("== get args: %v", args)

	if tr.toT != StrKey || tr.fromT != HashKey {
		return fmt.Errorf("unsupported key tr, now support from hash key to str key")
	}

	var err error

	if len(args) == 1 && args[0] == "-" {
		// support from shell stdin
		log.Infof("Reading hash keys from /dev/stdin ...")
		err = tr.TrStdinKeyFromHashKeyToStr()
	} else if len(args) >= 1 && args[0] != "-" {
		// support position args
		log.Infof("Reading hash keys from postional args ...")
		err = tr.TrArgsFromHashKeyToStr(args)
	} else if len(tr.loadF) >= 1 {
		// support from files load
		log.Infof("Reading hash keys from files %s with worker number %d ...", tr.loadF, tr.workerNum)
		err = tr.TrHkeysFromFiles(tr.loadF, tr.workerNum, tr.progress)
	} else {
		err = fmt.Errorf("invalid params")
	}

	if err != nil {
		log.Errorf("tr keys err: %v", err)
	} else {
		log.Infof("Tr keys success")
	}

	return err
}
