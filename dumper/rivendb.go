package dumper

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/douban/gobeansdb/quicklz"
)

const (
	FLAG_COMPRESS = 0x00010000
)

func crc32hash(s []byte) uint32 {
	hash := crc32.NewIEEE()
	hash.Write(s)
	return hash.Sum32()
}

type Record struct {
	Crc       uint32
	Timestamp uint32
	Flag      uint32
	Version   uint32
	Ksz       uint32
	Vsz       uint32
	Key       string
	Value     []byte
}

func (rec *Record) MetaString() string {
	return fmt.Sprintf("%d %d %d %d %d %s", rec.Timestamp, rec.Flag, rec.Version, rec.Ksz, rec.Vsz, rec.Key)
}

func ReadHead(rec *Record, data []byte) (err error) {
	b := bytes.NewBuffer(data)
	err = binary.Read(b, binary.LittleEndian, &(rec.Crc))
	err = binary.Read(b, binary.LittleEndian, &(rec.Timestamp))
	err = binary.Read(b, binary.LittleEndian, &(rec.Flag))
	err = binary.Read(b, binary.LittleEndian, &(rec.Version))
	err = binary.Read(b, binary.LittleEndian, &(rec.Ksz))
	err = binary.Read(b, binary.LittleEndian, &(rec.Vsz))
	return err
}

func ReadRecordAtPath(path string, offset uint32) (*Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ReadRecordAt(f, offset)
}

func (rec *Record) getSize() uint32 {
	recSize := 24 + rec.Ksz + rec.Vsz
	if recSize & 0xff != 0 {
		recSize = ((recSize >> 8) + 1) << 8
	}
	return recSize
}

func (rec *Record) decode(data []byte) (err error) {
	rec.Key = string(data[24 : 24+rec.Ksz])
	if rec.Crc != crc32hash(data[4:24+rec.Ksz+rec.Vsz]) {
		return fmt.Errorf("crc check fail")
	}
	v := data[24+rec.Ksz : 24+rec.Ksz+rec.Vsz]
	if (rec.Flag & FLAG_COMPRESS) != 0 {
		d, err := quicklz.CDecompressSafe(v)
		if err != nil {
			return err
		}
		rec.Flag -= FLAG_COMPRESS
		rec.Value = d.Body
	} else {
		rec.Value = v
	}
	return nil
}

func ReadRecordNext(f *os.File) (*Record, error) {
	data := make([]byte, 256)
	n_, err := f.Read(data)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, fmt.Errorf("read 256 err: %s", err)
	}
	
	if n_ < 24 {
		return nil, nil
	}

	rec := new(Record)
	err = ReadHead(rec, data)
	if err != nil {
		return nil, err
	}
	recSize := rec.getSize()

	if recSize > 256 {
		remainLen := recSize - 256
		restData := make([]byte, remainLen)
		n, err := f.Read(restData)
		if err != nil {
			return nil, err
		}
		if n != int(remainLen) {
			return nil, nil
		}

		tmp := make([]byte, recSize)
		i := 0
		i += copy(tmp[i:], data)
		copy(tmp[i:], restData)
		data = tmp
	}
	err = rec.decode(data)
	return rec, err
}

func ReadRecordAt(f *os.File, offset uint32) (*Record, error) {
	var data_ [256]byte
	data := data_[:256]
	_, err := f.ReadAt(data, int64(offset))
	if err != nil {
		return nil, err
	}
	rec := new(Record)
	err = ReadHead(rec, data)
	if err != nil {
		return nil, err
	}
	recSize := rec.getSize()
	if recSize > 256 {
		tmp := make([]byte, recSize)
		copy(tmp, data)
		_, err := f.ReadAt(tmp[256:], int64(offset)+256)
		if err != nil {
			return nil, err
		}
		data = tmp
	}
	rec.decode(data)
	return rec, nil
}
