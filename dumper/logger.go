package dumper

import (
	"fmt"
	"path/filepath"
	"strings"

	logrus "github.com/sirupsen/logrus"
	rotateLogger "gopkg.in/natefinch/lumberjack.v2"
)

var (
	log = logrus.New()
	dumpLogger = logrus.New()
)

const (
	dumpFilePatternFormat = "dump_key_btk_%s.%skey"
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

type DumpFileMgr struct {
	DumpFile string
	DumpLogger *logrus.Logger
}

func NewDumpFileMgr(dbpathRaw, dumpTo, logLevel *string, rotateSize *int, keyType KeyDumpType) (*DumpFileMgr, error) {
	setLogLevel(*logLevel)
	dbpath := strings.Split(*dbpathRaw, "/")
	log.Infof("dbpath : %v | %v\n", dbpathRaw, dbpath)

	// check if target folder already has file
	patternPath := []string{}
	for _, dpath := range dbpath {
		patternPath = append(patternPath, dpath)
	}
	patternPath[len(patternPath)-1] = fmt.Sprintf("%s*", patternPath[len(patternPath)-1])

	var kt string
	switch keyType {
	case HashKey:
		kt = "hash"
	case StrKey:
		kt = "str"
	case NurlKey:
		kt = "nurl"
	default:
		return nil, fmt.Errorf("unsupport keytyle: %v", keyType)
	}

	dumpTargetPattern := filepath.Join(
		*dumpTo,
		fmt.Sprintf(
			dumpFilePatternFormat,
			filepath.Join(patternPath...),
			kt,
		),
	)
	log.Infof("checking target file path: %s", dumpTargetPattern)
	matches, err := filepath.Glob(dumpTargetPattern)
	if err != nil {
		return nil, fmt.Errorf("glob %s err: %s", dumpTargetPattern, err)
	}
	if len(matches) > 0 {
		return nil, fmt.Errorf("there is dumped file exists already, clean it then retry or change a new folder: %v", matches)
	}

	// set dump Logger
	dumpLogger.SetFormatter(&DumpKeyFormatter{logrus.TextFormatter{
		DisableQuote: true,
		DisableColors: true,
		DisableSorting: true,
		DisableTimestamp: true,
		DisableLevelTruncation: true,
	}})
	dumpFile := filepath.Join(*dumpTo, fmt.Sprintf(dumpFilePatternFormat, *dbpathRaw, kt))
	dumpLogger.SetOutput(&rotateLogger.Logger{
		Filename: dumpFile,
		MaxSize: *rotateSize,
	})

	return &DumpFileMgr{
		DumpFile: dumpFile,
		DumpLogger: dumpLogger,
	}, nil
}
