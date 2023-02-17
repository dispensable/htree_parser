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
	// dumpKeyCmd represents the dumpKeyHash command
	var (	
		BktNumbers *int 
		HtreeHeight *int 
		DBHashFile *string 
		dbpathRaw *string 
		keyStart *int 
		keyLimit *int 
		keyPattern *string 
		dbAddr *string 
		dbPort *int 
		dumpTo *string 
		rotateSize *int 
		sleepInterval *int 
		loggerLevel *string 
		progress *int
		keyType *int
		cfgFile *string
	)

	dumpKeyCmd := &cobra.Command{
		Use:   "dumpKey",
		Short: "Dump key from hash tree file",
		Long: `Default Use this tool to dump keyhash file from beansdb's bucket .hash file.
All this cmd need is the bucket path and hash file(no .data required)

And this is very fast cause basicly it just dump things alreay loaded into the mem.

Note this is NOT key string(human readble) dumper default. If you need key string, you should
use dumpKey -t 1 cmd or use trHkeytokeystr from your hash key files.
`,
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

			// set logger and file dumper
			d, err := dumper.NewKeyDumper(
				BktNumbers, HtreeHeight, keyStart, keyLimit, dbPort, rotateSize, sleepInterval, progress,
				DBHashFile, dbpathRaw, keyPattern, dbAddr, dumpTo, loggerLevel, cfgFile,
				kt,
			)
			if err != nil {
				return err
			}
			return d.DumpKeys()
		},
	}

	// Here you will define your flags and configuration settings.
	flag := dumpKeyCmd.Flags()
	BktNumbers = flag.IntP("btk-number", "b", 256, "number of beansdbs bucket (/etc/gobeansdb/route.yaml)")
	HtreeHeight = flag.IntP("htree-height", "H", 6, "height of htree: /etc/gobeansdb/global.yaml -> htree_height")
	DBHashFile = flag.StringP("hash-file", "f", "/var/lib/beansdb/x/x/*.hash", "hash file")
	dbpathRaw = flag.StringP("db-path", "p", "", "db bucket path, eg a/b bucket")
	keyStart = flag.IntP("start", "s", 0, "key hash cnt start from")
	keyLimit = flag.IntP("limit", "l", 100, "key hash cnt limit")
	keyPattern = flag.StringP("key-pattern", "k", "", "if set only dump key string match this regex")
	cfgFile = flag.StringP("cfg", "c", "", "cfg file for dump")
	dbAddr = flag.StringP("db-addr", "d", "127.0.0.1", "beansdb addr")
	dbPort = flag.IntP("db-port", "P", 7900, "beansdb port")
	dumpTo = flag.StringP("dump-to-dir", "D", "./", "dump to dir")
	rotateSize = flag.IntP("max-file-size-mb", "S", 500, "rotate file when dump file size over this throshold, MB")
	sleepInterval = flag.IntP("sleep-interval-ms", "i", 1000, "sleep N ms during each key get")
	loggerLevel = flag.StringP("log-level", "L", "info", "log level: info warn error fatal debug trace")
	progress = flag.IntP("progress", "g", 1000, "show progress every N lines, 0 means no progress")
	keyType = flag.IntP("key-type", "t", 0, "dumped key type, 0: hash key 1: string key")

	dumpKeyCmd.MarkFlagRequired("key-type")
	dumpKeyCmd.MarkFlagRequired("hash-file")
	
	rootCmd.AddCommand(dumpKeyCmd)
}
