package dumper

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type stFuncT func(string, *KeyFinder, *KeyFinder, bool) error

type StressStatus struct {
	HandledF []string `json:"handled_files"`
	CurrentF string `json:"current_file"`
	CurrentKey string `json:"current_key"`
	SafeKeyLineCnt int `json:"safe_key_line_cnt"`
}

type StressUtils struct {
	dbAddr string
	dbPort uint16
	todbAddr string
	todbPort uint16
	dumpFMgr *DumpFileMgr
	loadF []string

	workerNum int
	progress int
	sleepInterval int
	retries int

	action string

	// stress func
	// get -> set
	// get
	stFunc stFuncT
	dumpErrorKey bool
	production bool

	// read proportion
	r int

	// status file
	statusF string
	status *StressStatus

	// key random
	keyrandom bool
}

func NewStressUtils(
	dbAddr, todbAddr, action, dbpathRaw, dumpTo, loggerLevel, statusF *string,
	loadFromFiles *[]string,
	dbPort, todbPort uint16,
	sleepInterval, progress, workerNum, retries, readProportion, rotateSize *int,
	dumpErrorKey, production *bool,
) (*StressUtils, error) {

	st := new(StressUtils)
	st.dbAddr = *dbAddr
	st.todbAddr = *todbAddr
	st.dbPort = dbPort
	st.todbPort = *&todbPort

	st.loadF = *loadFromFiles
	st.progress = *progress
	st.workerNum = *workerNum
	st.sleepInterval = *sleepInterval
	st.retries = *retries
	st.dumpErrorKey = *dumpErrorKey
	st.production = *production
	st.statusF = *statusF
	st.keyrandom = true

	if st.statusF != "" {
		if exists, _ := IsPathExists(st.statusF); exists {
			status, err := LoadStatus(st.statusF)
			if err != nil {
				return nil, fmt.Errorf("Load status from %s err: %s", st.statusF, err)
			}
			st.status = status
		} else {
			st.status = new(StressStatus)
		}
	}

	if st.dumpErrorKey {
		mgr, err := NewDumpFileMgr(dbpathRaw, dumpTo, loggerLevel, rotateSize, ErrorKey, nil)
		if err != nil {
			return nil, err
		}
		st.dumpFMgr = mgr
	}
	
	st.action = *action
	st.r = *readProportion

	switch st.action {
	case "getset":
		st.stFunc = stGetSet
	case "getrset":
		st.stFunc = stGetRSet
	case "get":
		st.stFunc = stGet
	case "rget":
		st.stFunc = stRGet
	case "getcmp":
		st.stFunc = stGetCmp
	case "getrcmp":
		st.stFunc = stGetRCmp
	case "sync":
		st.stFunc = stSync
	case "getsetp", "getsetpr":
		randKey := st.action == "getsetpr"
		f, err := getFuncByRW(st.r, randKey)
		if err != nil {
			return nil, err
		}
		st.stFunc = f
	case "deleter":
		st.stFunc = stDeleteR
	case "delete":
		st.stFunc = stDelete
	case "setrandom":
		st.stFunc = setRandom
	case "setrandomr":
		st.stFunc = setRandomR
	default:
		return nil, fmt.Errorf("unsupported action: %s", st.action)
	}

	return st, nil
}

func sliceContains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func LoadStatus(statusF string) (*StressStatus, error) {
	status, err := ioutil.ReadFile(statusF)
	if err != nil {
		return nil, err
	}

	var s StressStatus
	if err = json.Unmarshal(status, &s); err != nil {
		return nil, err
	}

	return &s, nil
}

func (ss *StressStatus) PickKeyFiles(loadFiles []string) ([]string, []string, int, error) {
	result := []string{ss.CurrentF}
	handled := []string{}
	shouldBeTrue := false

	for _, f := range loadFiles {
		if ss.CurrentF == f {
			shouldBeTrue = true
			continue
		}

		if sliceContains(ss.HandledF, f) {
			handled = append(handled, f)
		} else {
			result = append(result, f)
		}
	}

	if !shouldBeTrue {
		return nil, nil, 0, fmt.Errorf("status seems broken, if loadf contains this status ?")
	}

	// handle current file to drop processed line
	file, err := os.Open(ss.CurrentF)
	if err != nil {
		return nil, nil, 0, err
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	startCnt := 0
	for scanner.Scan() {
		startCnt += 1
		if scanner.Text() == ss.CurrentKey {
			if startCnt - ss.SafeKeyLineCnt < 0 {
				// means we are handle the last file
				startCnt = 0
			} else {
				startCnt -= ss.SafeKeyLineCnt
			}
			return result, handled, startCnt, nil
		}
	}

	return nil, nil, 0, fmt.Errorf("can't find status key %s in file: %s", ss.CurrentKey, ss.CurrentF)
}

func IsPathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (ss *StressStatus) Save(toF string) {
	c, err := json.MarshalIndent(ss, "", "  ")
	if err != nil {
		log.Errorf("marshal status failed: %v | %v", ss, err)
		return
	}

	// print old status
	// what if we passed wrong params
	var f *os.File
	if exists, _ := IsPathExists(toF); exists {
		log.Warnf("%s already exists, will overwrite it", toF)

		oldC, err := ioutil.ReadFile(toF)
		if err != nil {
			log.Errorf("read %s failed", toF)
		} else {
			log.Warnf("old content of flie %s\n%s\n", toF, oldC)
		}

		f, err = os.OpenFile(toF, os.O_TRUNC|os.O_WRONLY, 0755)
	} else {
		f, err = os.Create(toF)
	}

	if err != nil {
		log.Errorf("can't create/write to file: %s \n%s, err: %s", toF, c, err)
		return
	}
	// write to file
	defer f.Close()

	log.Infof("new status write:\n%s", c)
	_, err = f.Write(c)
	if err != nil {
		log.Errorf("write to file %s err: %s\n%s", toF, err, c)
	}
}

func (st *StressUtils) GetKeysAndAct(files []string, workerNum int, progress int) error {
	var wg sync.WaitGroup
	var consumerChans []chan string
	
	var fromKeyFinders []*KeyFinder
	var toKeyFinders []*KeyFinder
	chanBufferLen := 10
	
	for i := 0; i <= workerNum-1; i++ {
		wg.Add(1)

		log.Infof("Adding worker number: %d", i)
		c := make(chan string, chanBufferLen)
		consumerChans = append(consumerChans, c)

		fkf, err := NewKeyFinder(st.dbAddr, st.dbPort, st.retries)
		if err != nil {
			return fmt.Errorf("create from keyfinder err: %v", err)
		}

		fromKeyFinders = append(fromKeyFinders, fkf)

		tkf, err := NewKeyFinder(st.todbAddr, st.todbPort, st.retries)
		if err != nil {
			return fmt.Errorf("create to keyfinder err: %v", err)
		}

		toKeyFinders = append(toKeyFinders, tkf)

		go func(fkf, tkf *KeyFinder, taskChan chan string, idx int, prod bool) {
			defer wg.Done()
			log.Infof("consumer %d started ...", idx)
			total := 0
			errored := 0
			for t := range taskChan {
				err := st.stFunc(t, fkf, tkf, prod)
				if err != nil {
					log.Infof("run %s on key %s err: %v", st.action, t, err)
					if st.dumpErrorKey {
						st.dumpFMgr.DumpLogger.Info(t)
					}
					errored += 1
					continue
				}

				total += 1
				if progress != 0 {
					if total % progress == 0 {
						log.Infof("worker %d consumed %d records ... err cnt: %d", idx, total, errored)
					}
				}
			}
			log.Infof("consumer %d exit, total: %d, error: %d", idx, total, errored)
		}(fkf, tkf, c, i, st.production)
	}

	// save satus if ctrl-c
	sigc := make(chan os.Signal)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigc
		if st.status != nil {
			st.status.Save(st.statusF)
		}
	    os.Exit(1)
	}()

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
		var startCnt int
		if st.statusF != "" && st.status != nil {
			if st.status.CurrentF != "" {
				waitForHandle, handled, cnt, err := st.status.PickKeyFiles(files)
				if err != nil {
					log.Errorf("check status file err: %s", err)
					return
				}
				st.status.HandledF = handled
				files = waitForHandle
				startCnt = cnt
			} else {
				// fresh status need init
				st.status.HandledF = []string{}
			}
			// previous keys may need re handle
			// + n (>1) to prevent -1 err
			// we don't care to replay some keys
			st.status.SafeKeyLineCnt = chanBufferLen * st.workerNum + 10
			defer st.status.Save(st.statusF)
		}

		skipLineCnt := 0
		for _, f := range files {
			log.Infof(">> produce hkeys from file: %s", f)
			file, err := os.Open(f)
			defer file.Close()
			if err != nil {
				log.Errorf("open hkeys file %s err: %s", f, err)
				continue
			}

			scanner := bufio.NewScanner(file)

			// handle restart from last tasks
			if st.status != nil {
				// we need skip lines processed
				if startCnt != 0 && f == files[0] {
					for scanner.Scan() {
						skipLineCnt++
						if skipLineCnt > startCnt {
							log.Warnf("skipped %s lines: %d", f, skipLineCnt)
							break
						}
					}
				}
				st.status.CurrentF = f
			}

			chIdxStart := 0
			for scanner.Scan() {
				txt := scanner.Text()
				consumerChans[chIdxStart] <- txt
				if st.status != nil {
					st.status.CurrentKey = txt
				}
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

			// wait for consumer consume then proceed
			for {
				shouldNextFile := true
				for _, c := range consumerChans {
					if len(c) != 0 {
						shouldNextFile = false
						break
					}
				}

				if shouldNextFile {
					break
				} else {
					time.Sleep(1 * time.Second)
				}
			}

			if st.status != nil {
				st.status.HandledF = append(st.status.HandledF, f)
			}
		}
		log.Infof("Added %d task records total", total)
		log.Infof("all records proccessed, closing ch buffer ...")
		// in here we have finished produce and all consumer chan empty
		// now we say all files consumed
		if st.status != nil {
			st.status.SafeKeyLineCnt = 0
		}
	}()

	log.Infof("waiting for task running ...")
	wg.Wait()
	return nil
}
