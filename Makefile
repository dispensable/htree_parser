build:
	@test -f htree_iter.go
	@mkdir -p ../gobeansdb
	@test -f ../gobeansdb/go.mod || https_proxy=http://douproxy:8118 git clone --branch master --depth 1 --single-branch https://github.com/douban/gobeansdb.git ../gobeansdb
	@cp -rfv ./htree_iter.go ../gobeansdb/store/
	@test -f ../gobeansdb/store/htree_iter.go
	@sed -i '1d' ../gobeansdb/store/htree_iter.go
	go build
