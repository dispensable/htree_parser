module github.com/dispensable/htree_parser

go 1.19

require (
	github.com/dghubble/trie v0.0.0-20220811160003-18e0eff3ca7b
	github.com/douban/gobeansdb v1.1.2
	github.com/douban/gobeansproxy v0.0.0-00010101000000-000000000000
	github.com/douban/libmc v1.4.2
	github.com/sirupsen/logrus v1.9.0
	github.com/spf13/cobra v1.6.1
	github.intra.douban.com/coresys/gorivendb v0.0.0-20200121083828-152df2ab040c
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/gocql/gocql v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/inconshreveable/mousetrap v1.0.1 // indirect
	github.com/samuel/go-zookeeper v0.0.0-20190923202752-2cc03de413da // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	golang.org/x/sync v0.3.0 // indirect
	golang.org/x/sys v0.8.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace github.com/douban/gobeansdb => ../gobeansdb

replace github.com/douban/gobeansproxy => github.com/dispensable/gobeansproxy v1.1000.2
