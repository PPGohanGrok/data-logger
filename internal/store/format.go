package store

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"
)

const (
	Magic      = "TSEG"
	Version    = uint16(1)
	HeaderSize = 128
)

type fileHeader struct {
	Magic      [4]byte
	Version    uint16
	FieldCount uint16
	RecordSize uint32
	SchemaHash uint64
	HourStart  int64
}

func hourStart(unixNano int64) int64 {
	return time.Unix(0, unixNano).UTC().Truncate(time.Hour).UnixNano()
}

func hourFromNames(day, name string) (int64, error) {
	if len(name) != len("15.seg") || name[2:] != ".seg" {
		return 0, fmt.Errorf("文件名 %s", name)
	}
	t, err := time.ParseInLocation("2006-01-02 15", day+" "+name[:2], time.UTC)
	if err != nil {
		return 0, err
	}
	return t.UnixNano(), nil
}

func segmentPath(dir string, hour int64) string {
	t := time.Unix(0, hour).UTC()
	return filepath.Join(dir, t.Format("2006-01-02"), t.Format("15")+".seg")
}

func (s *Store) headerFor(hour int64) fileHeader {
	var magic [4]byte
	copy(magic[:], Magic)
	return fileHeader{
		Magic:      magic,
		Version:    Version,
		FieldCount: uint16(s.fieldCount),
		RecordSize: uint32(s.recordSize),
		SchemaHash: s.hash,
		HourStart:  hour,
	}
}

func encodeHeader(h fileHeader) []byte {
	buf := make([]byte, HeaderSize)
	copy(buf[0:4], h.Magic[:])
	binary.LittleEndian.PutUint16(buf[4:6], h.Version)
	binary.LittleEndian.PutUint16(buf[6:8], h.FieldCount)
	binary.LittleEndian.PutUint32(buf[8:12], h.RecordSize)
	binary.LittleEndian.PutUint64(buf[12:20], h.SchemaHash)
	binary.LittleEndian.PutUint64(buf[20:28], uint64(h.HourStart))
	return buf
}

func decodeHeader(buf []byte) fileHeader {
	var h fileHeader
	copy(h.Magic[:], buf[0:4])
	h.Version = binary.LittleEndian.Uint16(buf[4:6])
	h.FieldCount = binary.LittleEndian.Uint16(buf[6:8])
	h.RecordSize = binary.LittleEndian.Uint32(buf[8:12])
	h.SchemaHash = binary.LittleEndian.Uint64(buf[12:20])
	h.HourStart = int64(binary.LittleEndian.Uint64(buf[20:28]))
	return h
}

func readHeader(f *os.File) (fileHeader, error) {
	var buf [HeaderSize]byte
	_, err := f.ReadAt(buf[:], 0)
	if err != nil {
		return fileHeader{}, err
	}
	return decodeHeader(buf[:]), nil
}

// Encode 把一条记录写成小端字节：int64 纳秒时间戳，随后是 float64。
func Encode(ts int64, values []float64) []byte {
	buf := make([]byte, 8+len(values)*8)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(ts))
	for i, v := range values {
		binary.LittleEndian.PutUint64(buf[8+i*8:], math.Float64bits(v))
	}
	return buf
}

// Decode 解析一条 Encode 产生的记录。字段个数由字节长度决定。
func Decode(b []byte) (Record, error) {
	if len(b) < 8 || (len(b)-8)%8 != 0 {
		return Record{}, fmt.Errorf("记录长度不正确")
	}
	return decodeRecord(b, (len(b)-8)/8), nil
}

func decodeRecord(b []byte, n int) Record {
	vals := make([]float64, n)
	ts := int64(binary.LittleEndian.Uint64(b[:8]))
	for i := 0; i < n; i++ {
		vals[i] = math.Float64frombits(binary.LittleEndian.Uint64(b[8+i*8:]))
	}
	return Record{UnixNano: ts, Values: vals}
}

func searchFirst(buf []byte, recSize int, target int64) int {
	n := len(buf) / recSize
	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		ts := int64(binary.LittleEndian.Uint64(buf[mid*recSize:]))
		if ts < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}
