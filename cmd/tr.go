/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"path/filepath"

	"github.com/dispensable/htree_parser/dumper"
	"github.com/spf13/cobra"
)

func init() {
// trHkeyToKeystrCmd represents the trHkeyToKeystr command
	var trCmd = &cobra.Command{
		Use:   "tr",
		Short: "tr key from type to annother type",
		Long: `translate key from type to type

htree_parser tr -f 0 -t 1 -F <files contains from type>
means from type hash to type str
`,
	}

	flag := trCmd.Flags()
	var dbAddr *string = flag.StringP("db-addr", "d", "127.0.0.1", "beansdb addr")
	var dbPort *uint16 = flag.Uint16P("db-port", "P", 7900, "beansdb port")
	var fromT *int = flag.IntP("from", "f", 0, "from type, 0: hash key type")
	var toT *int = flag.IntP("to", "t", 1, "to type: 1: str key type")
	var loadFromFiles *string = flag.StringP(
		"load-from-file", "F", "",
		"load from type key from file path, support glob, remember use single quote in shell '")
	var dumpTo *string = flag.StringP("dump-to-dir", "D", "./", "dump to dir")
	var dbpathRaw *string = flag.StringP("db-path", "p", "", "db bucket path, eg a/b bucket")
	var keyPattern *string = flag.StringP("key-pattern", "k", "", "if set only dump key string match this regex")
	var cfgFile *string = flag.StringP("cfg", "c", "", "cfg file for dump")
	var rotateSize *int = flag.IntP("max-file-size-mb", "S", 500, "rotate file when dump file size over this throshold, MB")
	var sleepInterval *int = flag.IntP("sleep-interval-ms", "i", 1000, "sleep N ms during each key get")
	var loggerLevel *string = flag.StringP("log-level", "L", "info", "log level: info warn error fatal debug trace")
	var progress *int = flag.IntP("progress", "g", 1000, "show progress every N lines, 0 means no progress(only support tr from file)")
	var workerNum *int = flag.IntP("worker-num", "w", 1, "only support tr from file")
	var retries *int = flag.IntP("retries", "R", 3, "retry times when libmc err")

	trCmd.RunE = func(cmd *cobra.Command, args []string) error {
		matches, err := filepath.Glob(*loadFromFiles)
		if err != nil {
			return err
		}

		trU, err := dumper.NewTrKeyUtils(
			dbAddr, dumpTo, dbpathRaw, keyPattern, loggerLevel, cfgFile,
			&matches,
			dumper.KeyDumpType(*fromT), dumper.KeyDumpType(*toT),
			uint16(*dbPort),
			rotateSize, sleepInterval, progress, workerNum, retries,
		)
		if err != nil {
			return err
		}

		// tr key from type to type
		return trU.Tr(args)
	}
	rootCmd.AddCommand(trCmd)
}
