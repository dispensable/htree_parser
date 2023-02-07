build:
	test -f htree_iter.go
	https_proxy=http://douproxy:8118 go mod vendor
	cp -rf vendor/github.com/douban/gobeansdb ../
	rm -rf vendor
	cp -rfv ./htree_iter.go ../gobeansdb/store/
	test -f ../gobeansdb/store/htree_iter.go
	sed -i '1d' ../gobeansdb/store/htree_iter.go
	go build
