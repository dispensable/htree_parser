module github.com/dispensable/htree_parser

go 1.19

require (
	github.com/douban/gobeansdb v0.0.0-00010101000000-000000000000
	github.com/douban/libmc v1.4.2
	github.com/sirupsen/logrus v1.9.0
	github.com/spf13/pflag v1.0.5
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
)

require (
	github.com/dghubble/trie v0.0.0-20220811160003-18e0eff3ca7b // indirect
	github.com/inconshreveable/mousetrap v1.0.1 // indirect
	github.com/samuel/go-zookeeper v0.0.0-20190923202752-2cc03de413da // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cobra v1.6.1 // indirect
	golang.org/x/sys v0.0.0-20220715151400-c0bba94af5f8 // indirect
	gopkg.in/yaml.v2 v2.2.7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/douban/gobeansdb => ../gobeansdb
