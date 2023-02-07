This is a cmd tool for gobeansdb hash tree file parsing.


# usage
```
Usage of ./htree_parser:
  -b, --btk-bumber int          number of beansdbs bucket (/etc/gobeansdb/route.yaml) (default 256)
  -d, --db-addr string          beansdb addr (default "127.0.0.1")
  -p, --db-path string          db bucket path, eg a/b bucket
  -P, --db-port int             beansdb port (default 7900)
  -D, --dump-to-dir string      dump to dir (default "./")
  -f, --hash-file string        hash file (default "/var/lib/beansdb/x/x/*.hash")
  -H, --htree-height int        height of htree: /etc/gobeansdb/global.yaml -> htree_height (default 6)
  -l, --limit int               key hash cnt limit (default 100)
  -L, --log-level string        log level: info warn error fatal debug trace (default "info")
  -S, --max-file-size-mb int    rotate file when dump file size over this throshold, MB (default 500)
  -g, --progress int            show progress every N lines, 0 means no progress (default 1000)
  -i, --sleep-interval-ms int   sleep N ms during each key get (default 1000)
  -s, --start int               key hash cnt start from
pflag: help requested
```

# build

```
make build
```
