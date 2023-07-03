package dumper

import (
	"bufio"
	"fmt"
	"os"
	"sync"

	"math/rand"
)

type stFuncT func(string, *KeyFinder, *KeyFinder) error

type StressUtils struct {
	dbAddr string
	dbPort uint16
	todbAddr string
	todbPort uint16
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

	// read proportion
	r int
}

func stGetSet(key string, fromFinder, toFinder *KeyFinder) error {
	item, err := fromFinder.client.Get(key)
	if item == nil {
		return nil
	}

	if err != nil {
		return err
	}

	return toFinder.client.Set(item)
}

func stGet(key string, fromFinder, toFinder *KeyFinder) error {
	item, err := toFinder.client.Get(key)
	if item == nil {
		return nil
	}
	return err
}

func stGetCmp(key string, fromFinder, toFinder *KeyFinder) error {
	itemF, errf := fromFinder.client.Get(key)
	itemT, errt := toFinder.client.Get(key)

	if errf != nil {
		return errf
	}

	if errt != nil {
		return errt
	}
	
	if itemF == nil && itemT == nil {
		return nil
	}

	if itemF == nil || itemT == nil {
		return fmt.Errorf("itemF: %s itemT: %s | not all nil", itemF, itemT)
	}

	if string(itemF.Value) == string(itemT.Value) {
		return nil
	}

	return fmt.Errorf("itemF and itemT not equal: %s || %s", itemF, itemT)
}

func getFuncByRW(r int) (stFuncT, error) {
	if !(r < 10) {
		return nil, fmt.Errorf("r must < 10")
	}

	return func(key string, fkf, tkf *KeyFinder) error {
		if rand.Intn(10) < r {
			return stGet(key, fkf, tkf)
		} else {
			return stGetSet(key, fkf, tkf)
		}
	}, nil
}

func NewStressUtils(
	dbAddr, todbAddr, action *string,
	loadFromFiles *[]string,
	dbPort, todbPort uint16,
	sleepInterval, progress, workerNum, retries, readProportion *int,
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

	st.action = *action
	st.r = *readProportion

	switch st.action {
	case "getset":
		st.stFunc = stGetSet
	case "get":
		st.stFunc = stGet
	case "getcmp":
		st.stFunc = stGetCmp
	case "getsetp":
		f, err := getFuncByRW(st.r)
		if err != nil {
			return nil, err
		}
		st.stFunc = f
	default:
		st.stFunc = stGetSet
	}

	return st, nil
}

func (st *StressUtils) GetKeysAndAct(files []string, workerNum int, progress int) error {
	var wg sync.WaitGroup
	var consumerChans []chan string
	
	var fromKeyFinders []*KeyFinder
	var toKeyFinders []*KeyFinder
	
	for i := 0; i <= workerNum-1; i++ {
		wg.Add(1)

		log.Infof("Adding worker number: %d", i)
		c := make(chan string, 10)
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

		go func(fkf, tkf *KeyFinder, taskChan chan string, idx int) {
			defer wg.Done()
			log.Infof("consumer %d started ...", idx)
			total := 0
			errored := 0
			for t := range taskChan {
				err := st.stFunc(t, fkf, tkf)
				if err != nil {
					log.Debugf("run %s on key %s err: %v", st.action, t, err)
					errored += 1
					continue
				}

				total += 1
				if progress != 0 {
					if total % progress == 0 {
						log.Infof("worker %d consumed %d records ...", idx, total)
					}
				}
			}
			log.Infof("consumer %d exit, total: %d, error: %d", idx, total, errored)
		}(fkf, tkf, c, i)
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
