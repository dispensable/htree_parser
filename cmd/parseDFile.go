/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/dispensable/htree_parser/dumper"
)

func init() {
	var (
		DBDataFile *string
		keyLimit *int
		keyPattern *string
		sleepInterval *int
		progress *int
		keyType *int
		cfgFile *string
		dbAddr *string
		dbPort *uint16
		writeToCstar *bool
		enableProf *bool
		loggerLevel *string
		dbpathraw *string
		dumpTo *string
		rotateSize *int
		workerNum *int
	)

	parseDataFileCmd := &cobra.Command{
		Use:   "parseDataFile",
		Short: "parse beansdb datafile to cassandra",
		Long: `parse gobeansdb datafiel and save to cassandra`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var kt dumper.KeyDumpType

			switch int64(*keyType) {
			case int64(dumper.HashKey):
				kt = dumper.HashKey
			case int64(dumper.StrKey):
				kt = dumper.StrKey
			default:
				return fmt.Errorf("unsupport key type: %d", *keyType)
			}

			// logic goes here
			parser, err := dumper.NewDataFileParser(
				DBDataFile, keyPattern, cfgFile, dbAddr, dbpathraw, dumpTo, loggerLevel,
				keyLimit, sleepInterval, progress, rotateSize, workerNum,
				kt, dbPort, writeToCstar,
			)
			if err != nil {
				return err
			}
			return parser.Parse(*enableProf)
		},
	}

	// Here you will define your flags and configuration settings.
	flag := parseDataFileCmd.Flags()
	DBDataFile = flag.StringP("db-data-file", "f", "/var/lib/beansdb/x/x/*.data.*", "data file support glob")
	keyLimit = flag.IntP("limit", "l", 100, "key hash cnt limit")
	keyPattern = flag.StringP("key-pattern", "k", "", "if set only parse key string matched this regex")
	cfgFile = flag.StringP("cfg", "c", "", "cfg file for dump")
	sleepInterval = flag.IntP("sleep-interval-ms", "i", 1000, "sleep N ms during each key get")
	progress = flag.IntP("progress", "g", 1000, "show progress every N lines, 0 means no progress")
	keyType = flag.IntP("key-type", "t", 0, "dumped key type, 0: hash key 1: string key")
	dbAddr = flag.StringP("db-addr", "d", "127.0.0.1", "beansdb addr")
	dbPort = flag.Uint16P("db-port", "p", 7900, "beansdb port")
	writeToCstar = flag.BoolP("write-to-cstar", "C", false, "direct write to cstar, ignore -d/-P")
	enableProf = flag.BoolP("enable-prof", "P", false, "enable profiling of parse")
	dumpTo = flag.StringP("dump-to-dir", "D", "./", "dump to dir")
	dbpathraw = flag.StringP("db-path", "b", "", "db bucket path, eg a/b bucket")
	rotateSize = flag.IntP("max-file-size-mb", "S", 500, "rotate file when dump file size over this throshold, MB")
	loggerLevel = flag.StringP("log-level", "L", "info", "log level: info warn error fatal debug trace")
	workerNum = flag.IntP("worker-num", "w", 1, "only support tr from file")

	parseDataFileCmd.MarkFlagRequired("key-type")
	parseDataFileCmd.MarkFlagRequired("db-data-file")

	rootCmd.AddCommand(parseDataFileCmd)
}
