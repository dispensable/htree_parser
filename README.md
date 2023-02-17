A cli tool for gobeansdb hash tree file parsing. You can dump all key's hash value through .hash file and translate to real string key(like redis `keys *` command). Use analysis tool for key pattern frequency statistic. 


# usage

1. sync beansdb bucket from prod db to localdb for analysis. If you are running test db or don't care db data io, ignore this step.
```bash
bash scripts/migrate_db.sh -b f/6 -h angmar35 -d data/db_dumper/db/ -C data/db_dumper/cfg/ -u beansdb -l 1m
```

2. start a local beansdb with synced bucket
```
# you can sync prod cfg from your cluster
# edit global.yaml with your data dir and logdir

# create a route.yaml for db route
cat > data/db_dumper/cfg/route.yaml <<EOF
numbucket: 256
main:
- addr: 127.0.0.1:7900
  buckets: [f6]
EOF

# !route addr host shoud stay same with global.yaml hostname
# start your db

sudo -u beansdb gobeansdb -confdir data/db_dumper/cfg/
```

3. use htree_parser cmd for key dump and analysis
```bash
# dump hash key only
./htree_parser dumpKey -p 9/9 -H 3 -f ../gobeansdb/testdb/9/9/*.hash -g 10 -l 10 -i 0 -c cfg.yaml 

# dump str key (slow) you can connect to other beansdb with dbaddr dbport opts
/htree_parser dumpKey -p 9/9 -H 3 -f ../gobeansdb/testdb/9/9/*.hash -t 1 -g 10 -l 10 -i 0 -c cfg.yaml 

# tr hash key to str key from files
./htree_parser tr -p 9/9 -g 200 -i 0 -w 9 -F dump_key_btk_9/*.hashkey

# tr hash key to str key from stdin (use -)
echo '99fc79f125aec87f' | ./htree_parser tr -p 9/9 -g 200 -i 0 -w 9 -

# tr hash key in positional args
./htree_parser tr -p 9/9 -g 200 -i 0 -w 9 99fc79f125aec87f

# analysis keypattern
# dump (.hashkey) -> tr (.strkey) -> analysis (.nurlkey)
./htree_parser analysis -D ./ -p 9/6 -F '../dump_result/dump_key_btk_f/*.keyfile' -c ~/cfg.yaml
```

# cfg

see `cfg.yaml.example` comments

# build

```
make build
```
