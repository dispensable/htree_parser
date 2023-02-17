/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"path/filepath"

	"github.com/dispensable/htree_parser/dumper"
	"github.com/spf13/cobra"
)

// analysisCmd represents the analysis command
var analysisCmd = &cobra.Command{
	Use:   "analysis",
	Short: "analysis key pattern",
	Long: ``,
}

func init() {

	flag := analysisCmd.Flags()
	var loadFromFiles *string = flag.StringP(
		"load-from-file", "F", "",
		"load from type key from file path, support glob, remember use single quote in shell '")
	var maxFieldLen *int = flag.IntP("max-field-len", "m", 15, "max field length(if over this length, will replace as *)")
	var dumpTo *string = flag.StringP("dump-to-dir", "D", "-", "dump to dir")
	var dbpathRaw *string = flag.StringP("db-path", "p", "", "db bucket path, eg a/b bucket")
	var rotateSize *int = flag.IntP("max-file-size-mb", "S", 500, "rotate file when dump file size over this throshold, MB")
	var loggerLevel *string = flag.StringP("log-level", "L", "info", "log level: info warn error fatal debug trace")
	var cfgFile *string = flag.StringP("cfg", "c", "./cfg.yaml", "config file for htree_parser")

	analysisCmd.RunE = func(cmd *cobra.Command, args []string) error {
		matches, err := filepath.Glob(*loadFromFiles)
		if err != nil {
			return err
		}
		analyzer, err := dumper.NewCounterMgr(&matches, *maxFieldLen, dumpTo, dbpathRaw, loggerLevel, cfgFile, rotateSize)
		if err != nil {
			return err
		}
		return analyzer.Count()
	}

	rootCmd.AddCommand(analysisCmd)
}
