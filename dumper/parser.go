package dumper

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	libmc "github.com/douban/libmc/src"
)

type KeyFinderTelnet struct {
	conn *Conn
	buff bytes.Buffer
}

type KeyFinder struct {
	client *libmc.Client
	retries int
}

func NewKeyFinderTelnet(addr string, port uint16) (*KeyFinderTelnet, error) {
	c, err := DialTimeout(addr, port, time.Second * 5)
	if err != nil {
		return nil, fmt.Errorf("get connection from %s:%d err: %s", addr, port, err)
	}

	buffer := bytes.Buffer{}
	c.Output = &buffer

	return &KeyFinderTelnet{
		conn: c,
		buff: buffer,
	}, nil
	
}

func NewKeyFinder(addr string, port uint16, retries int) (*KeyFinder, error) {
	servers := []string{fmt.Sprintf("%s:%d", addr, port)}
	noreply := false
	hashFunc := libmc.HashCRC32
	failover := false
	disableLock := false
	client := libmc.New(servers, noreply, "", hashFunc, failover, disableLock)
	client.ConfigTimeout(libmc.ConnectTimeout, time.Millisecond*300)
	client.ConfigTimeout(libmc.PollTimeout, time.Second*5)
	client.ConfigTimeout(libmc.RetryTimeout, time.Second*5)
	client.SetMaxRetries(retries)

	return &KeyFinder{
		client: client,
		retries: retries,
	}, nil
}

func (k *KeyFinder) GetKeyByHash(hash uint64) ([]byte, error) {
	var item *libmc.Item
	var err error
	
	for i := 0; i < k.retries; i++ {
		item, err = k.client.Get(fmt.Sprintf("@@%016x", hash))
		if err == nil {
			break
		}
	}

	if err != nil {
		return nil, err
	}
	// fmt.Printf("got item: %+v\n", item)
	
	data := item.Value
	dataLen := len(data)

	if dataLen < 24 {
		return nil, fmt.Errorf("%x hash got bad data: %v", hash, string(data))
	}
	
	keySize := binary.LittleEndian.Uint32(data[16:20])
	// fmt.Printf("key size: %v\n", keySize)
	if dataLen < 24+int(keySize) {
		return nil, fmt.Errorf("%x hash do not have enough data, key size: %v, data len: %v", hash, keySize, dataLen)
	}
	return data[24:24+keySize], nil
}

func (k *KeyFinder) Close() {
	k.client.Quit()
}
