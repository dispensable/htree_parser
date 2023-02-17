package dumper

import (
	"fmt"
	"testing"
)

func TestNurl(t *testing.T) {
	files := []string{}
	dumpTo := "-"
	dbpathRaw := ""
	rsize := 1
	cfgPath := "./cfg.yaml"
	mgr, err := NewCounterMgr(&files, 15, &dumpTo, &dbpathRaw, nil, &cfgPath, &rsize)
	if err != nil {
		t.Errorf("new counter mgr err: %s", err)
	}

	cfg, err := NewDumperCfgFromFile(cfgPath)
	if err != nil {
		t.Errorf("parse cfg %s err: %s", cfgPath, err)
	}
	rTO := cfg.Analysis.ReplacedTo
	
	tests := []struct{
		key, expect string
	}{
		{"/oauth2/access_token/d93e7800691f3a138bbb68013b5dae39/props", fmt.Sprintf("/oauth%s/access/token/%s/props", rTO, rTO)},
		{"db:key:accounts:register_info:nzhubiecan", fmt.Sprintf("db/key/accounts/register/info/%s", rTO)},
		{"a/abcd1234455/ccc", fmt.Sprintf("a/abcd%s/ccc", rTO)},
		{"dae|edhellond|explore/movie/tag/极限运动", fmt.Sprintf("dae/edhellond/explore/movie/tag/.+")},
	}

	for _, testKey := range tests {
		n := mgr.nurl(mgr.trToPath(testKey.key), testKey.key)
		if testKey.expect != n {
			t.Errorf("nurl %s expect %s, got: %s", testKey.key, testKey.expect, n)
		}
	}
}
