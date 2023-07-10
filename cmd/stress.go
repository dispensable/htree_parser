/*
Copyright © 2023
*/
package cmd

import (
	"path/filepath"

	"github.com/dispensable/htree_parser/dumper"
	"github.com/spf13/cobra"
)

func init() {
	var stcmd = &cobra.Command{
		Use:   "stress",
		Short: "get key from db and run action",
		Long: `make some stress to db

you can use this cmd to test bdb like database

support actions:

# get set with same key
- getset: get key from db and set to another db
- getcmp: get key from both db and compare
- get: get key from db
- getsetp: get/set by use specific proportion eg: -r=5 means 50% read 50% write
- sync: for each key, use fromdb as source, sync to tofinder
- delete: for each key, delete from --db-addr
- setrandm: for each key, set with random data

# get set with rand key (prefix with key length)
# this is for beansdb when key all from same bucket dump
- getrset
- rget
- getrcmp
- getsetpr
- deleter
- setrandmr
`,
	}

	flag := stcmd.Flags()
	var dbAddr *string = flag.StringP("db-addr", "d", "127.0.0.1", "beansdb addr")
	var dbPort *uint16 = flag.Uint16P("db-port", "P", 7900, "beansdb port")
	var todbAddr *string = flag.StringP("to-db-addr", "T", "127.0.0.1", "beansdb addr")
	var todbPort *uint16 = flag.Uint16P("to-db-port", "t", 7900, "beansdb port")
	var loadFromFiles *string = flag.StringP(
		"load-from-file", "F", "",
		"load from type key from file path, support glob, remember use single quote in shell '")
	var statusF *string = flag.StringP(
		"status-file", "f", "",
		"status file of process")
	var action *string = flag.StringP("action", "a", "getset", "action of stress: getset/getcmp/get/getsetp/sync")
	var readScale *int = flag.IntP("read-scale", "r", 5, "only make sense in action getsetp. this specifc read stress scale must < 10")
	var sleepInterval *int = flag.IntP("sleep-interval-ms", "i", 1000, "sleep N ms during each key get")
	var dbpathRaw *string = flag.StringP("db-path", "p", "", "db bucket path, eg a/b bucket")
	var rotateSize *int = flag.IntP("max-file-size-mb", "S", 500, "rotate file when dump file size over this throshold, MB")
	var loggerLevel *string = flag.StringP("log-level", "L", "info", "log level: info warn error fatal debug trace")
	var dumpTo *string = flag.StringP("dump-to-dir", "D", "./", "dump to dir")
	var progress *int = flag.IntP("progress", "g", 1000, "show progress every N lines, 0 means no progress(only support tr from file)")
	var workerNum *int = flag.IntP("worker-num", "w", 1, "only support tr from file")
	var retries *int = flag.IntP("retries", "R", 3, "retry times when libmc err")
	var dumpErrorKey *bool = flag.BoolP("dump-error-key", "e", false, "dump errorkeys also")
	var production *bool = flag.BoolP("production", "y", false, "if set, really run the action")

	stcmd.RunE = func(cmd *cobra.Command, args []string) error {
		matches, err := filepath.Glob(*loadFromFiles)
		if err != nil {
			return err
		}

		stU, err := dumper.NewStressUtils(
			dbAddr, todbAddr, action, dbpathRaw, dumpTo, loggerLevel, statusF,
			&matches,
			uint16(*dbPort), uint16(*todbPort),
			sleepInterval, progress, workerNum, retries, readScale, rotateSize,
			dumpErrorKey, production,
		)
		if err != nil {
			return err
		}

		return stU.GetKeysAndAct(matches, *workerNum, *progress)
	}
	rootCmd.AddCommand(stcmd)
}
