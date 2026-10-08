// Package store 把时序样本追加进按小时切分的等宽二进制文件。
// 一条样本先进入内存并通知订阅者，然后才写入文件并刷盘。
package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"tsdb/internal/schema"
)

var (
	ErrTimeRegression = errors.New("时间戳倒退")
	ErrFieldCount     = errors.New("字段数量与 schema 不一致")
	ErrSchemaMismatch = errors.New("段文件 schema 与当前配置不一致")
	ErrLocked         = errors.New("数据目录已被另一个进程占用")
	ErrClosed         = errors.New("存储已关闭")
)

// Record 是内存中的一条样本。Seq 只在进程内递增，不写入磁盘。
type Record struct {
	Seq      uint64
	UnixNano int64
	Values   []float64
}

// Options 控制目录、保留时间和写入钩子。
// OnSample 在样本对读取可见之后、刷盘之前调用，并且不得再进入 Store。
// BeforeSync 只给测试用来挡住刷盘。DisableSync 只给测试和基准跳过刷盘。
type Options struct {
	DataDir     string
	Fields      []string
	Retention   time.Duration
	CacheHours  int
	OnSample    func(Record)
	BeforeSync  func()
	DisableSync bool
}

// Stats 是健康检查用的瞬时计数。
type Stats struct {
	Segments     int
	Records      int
	LastUnixNano int64
	HasLast      bool
	FieldCount   int
	RecordSize   int
	SchemaHash   uint64
}

type segment struct {
	hourStart int64
	path      string
	buf       []byte
	count     int
	file      *os.File
}

type memState struct {
	seg        *segment
	bufLen     int
	count      int
	lastTs     int64
	hasLast    bool
	latest     Record
	hasLatest  bool
	seq        uint64
	prevActive *segment
	inserted   bool
}

type prepared struct {
	file  *os.File
	raw   []byte
	state memState
}

// Store 是单写多读的时序目录。
type Store struct {
	opts       Options
	hash       uint64
	fieldCount int
	recordSize int

	opMu      sync.Mutex
	mu        sync.RWMutex
	segs      []*segment
	active    *segment
	lastTs    int64
	hasLast   bool
	seq       uint64
	latest    Record
	hasLatest bool
	closed    bool
	lockFile  *os.File
}

// Open 打开或创建数据目录。同一目录不能被第二个进程打开。
func Open(opts Options) (*Store, error) {
	if err := schema.Validate(opts.Fields); err != nil {
		return nil, err
	}
	if opts.DataDir == "" {
		return nil, errors.New("缺少数据目录")
	}
	if opts.CacheHours < 1 {
		opts.CacheHours = 3
	}
	if opts.Retention <= 0 {
		opts.Retention = 14 * 24 * time.Hour
	}
	if err := os.MkdirAll(opts.DataDir, 0o755); err != nil {
		return nil, err
	}
	lock, err := acquireLock(filepath.Join(opts.DataDir, ".lock"))
	if err != nil {
		return nil, err
	}
	s := &Store{
		opts:       opts,
		hash:       schema.Hash(opts.Fields),
		fieldCount: len(opts.Fields),
		recordSize: schema.RecordSize(len(opts.Fields)),
		lockFile:   lock,
	}
	if err := s.load(); err != nil {
		_ = releaseLock(lock)
		return nil, err
	}
	return s, nil
}

// Append 写入一条样本。返回 nil 时，这条样本已经刷到磁盘。
func (s *Store) Append(ts int64, values []float64) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	p, rec, err := s.stage(ts, values)
	if err != nil {
		return err
	}
	if s.opts.OnSample != nil {
		s.opts.OnSample(rec)
	}
	return s.persist(p, true)
}

// AppendMany 追加一批时间不倒退的样本，只把最后一条推给实时订阅者，并在末尾刷一次盘。
// 校验失败时不会写入任何一条。写入过程中磁盘出错时，前面已经写入的样本会保留。
func (s *Store) AppendMany(recs []Record) error {
	if len(recs) == 0 {
		return nil
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if err := s.validateAll(recs); err != nil {
		return err
	}
	var last Record
	var lastP prepared
	for i, rec := range recs {
		p, staged, err := s.stage(rec.UnixNano, rec.Values)
		if err != nil {
			return err
		}
		if i < len(recs)-1 {
			if err := s.persist(p, false); err != nil {
				return err
			}
			continue
		}
		last = staged
		lastP = p
	}
	if s.opts.OnSample != nil {
		s.opts.OnSample(last)
	}
	return s.persist(lastP, true)
}

// Query 返回 [from, to) 内的样本，按时间升序。
func (s *Store) Query(from, to int64) ([]Record, error) {
	if to <= from {
		return []Record{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	out := make([]Record, 0)
	for _, seg := range s.segs {
		segEnd := seg.hourStart + int64(time.Hour)
		if segEnd <= from || seg.hourStart >= to {
			continue
		}
		buf, err := s.segmentBytes(seg)
		if err != nil {
			return nil, err
		}
		n := len(buf) / s.recordSize
		for i := searchFirst(buf, s.recordSize, from); i < n; i++ {
			off := i * s.recordSize
			ts := int64(binaryUint64(buf[off:]))
			if ts >= to {
				return out, nil
			}
			out = append(out, decodeRecord(buf[off:off+s.recordSize], s.fieldCount))
		}
	}
	return out, nil
}

// Latest 返回最新的 n 条，按时间升序。
func (s *Store) Latest(n int) ([]Record, error) {
	if n <= 0 {
		return []Record{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	rev := make([]Record, 0, n)
	for i := len(s.segs) - 1; i >= 0 && len(rev) < n; i-- {
		buf, err := s.segmentBytes(s.segs[i])
		if err != nil {
			return nil, err
		}
		count := len(buf) / s.recordSize
		for j := count - 1; j >= 0 && len(rev) < n; j-- {
			off := j * s.recordSize
			rev = append(rev, decodeRecord(buf[off:off+s.recordSize], s.fieldCount))
		}
	}
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	return rev, nil
}

// Last 返回内存中的最新一条。
func (s *Store) Last() (Record, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.hasLatest {
		return Record{}, false
	}
	return cloneRecord(s.latest), true
}

// Stats 返回段数、条数和最新时间。
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := Stats{
		Segments:     len(s.segs),
		FieldCount:   s.fieldCount,
		RecordSize:   s.recordSize,
		SchemaHash:   s.hash,
		HasLast:      s.hasLatest,
		LastUnixNano: s.latest.UnixNano,
	}
	for _, seg := range s.segs {
		st.Records += seg.count
	}
	return st
}

// RunRetention 删除结束时间早于 now-保留时长 的小时文件。
func (s *Store) RunRetention(now time.Time) (int, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	cutoff := now.Add(-s.opts.Retention).UnixNano()
	keep := make([]*segment, 0, len(s.segs))
	removed := 0
	dirs := make([]string, 0)
	for _, seg := range s.segs {
		if seg.hourStart+int64(time.Hour) <= cutoff {
			if seg.file != nil {
				_ = seg.file.Sync()
				_ = seg.file.Close()
				seg.file = nil
			}
			if s.active == seg {
				s.active = nil
			}
			if err := os.Remove(seg.path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return removed, err
			}
			dirs = append(dirs, filepath.Dir(seg.path))
			removed++
			continue
		}
		keep = append(keep, seg)
	}
	s.segs = keep
	if s.hasLatest {
		h := hourStart(s.latest.UnixNano)
		alive := false
		for _, seg := range s.segs {
			if seg.hourStart == h {
				alive = true
				break
			}
		}
		if !alive {
			s.recomputeLastLocked()
		}
	}
	for _, dir := range dirs {
		_ = os.Remove(dir)
	}
	return removed, nil
}

// RetentionLoop 启动时清一次，之后每分钟清一次，直到 ctx 结束。
func (s *Store) RetentionLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	s.retainOnce(time.Now())
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.retainOnce(now)
		}
	}
}

func (s *Store) retainOnce(now time.Time) {
	n, err := s.RunRetention(now)
	if err != nil && !errors.Is(err, ErrClosed) {
		log.Printf("清理过期数据: %v", err)
		return
	}
	if n > 0 {
		log.Printf("已删除 %d 个过期小时文件", n)
	}
}

// Close 刷盘、关闭文件并放开目录锁。
func (s *Store) Close() error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var err error
	for _, seg := range s.segs {
		if seg.file == nil {
			continue
		}
		if e := seg.file.Sync(); e != nil && err == nil {
			err = e
		}
		if e := seg.file.Close(); e != nil && err == nil {
			err = e
		}
		seg.file = nil
	}
	s.active = nil
	if s.lockFile != nil {
		if e := releaseLock(s.lockFile); e != nil && err == nil {
			err = e
		}
		s.lockFile = nil
	}
	return err
}

func (s *Store) validateAll(recs []Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	last := s.lastTs
	has := s.hasLast
	for _, rec := range recs {
		if len(rec.Values) != s.fieldCount {
			return ErrFieldCount
		}
		if has && rec.UnixNano < last {
			return ErrTimeRegression
		}
		last = rec.UnixNano
		has = true
	}
	return nil
}

func (s *Store) stage(ts int64, values []float64) (prepared, Record, error) {
	if len(values) != s.fieldCount {
		return prepared{}, Record{}, ErrFieldCount
	}
	values = append([]float64(nil), values...)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return prepared{}, Record{}, ErrClosed
	}
	if s.hasLast && ts < s.lastTs {
		return prepared{}, Record{}, ErrTimeRegression
	}

	hour := hourStart(ts)
	state := memState{
		lastTs:     s.lastTs,
		hasLast:    s.hasLast,
		latest:     s.latest,
		hasLatest:  s.hasLatest,
		seq:        s.seq,
		prevActive: s.active,
	}
	if s.active == nil || s.active.hourStart != hour {
		if s.active != nil && s.active.file != nil {
			if err := s.active.file.Sync(); err != nil {
				return prepared{}, Record{}, err
			}
			if err := s.active.file.Close(); err != nil {
				return prepared{}, Record{}, err
			}
			s.active.file = nil
		}
		seg, created, err := s.openHourLocked(hour)
		if err != nil {
			return prepared{}, Record{}, err
		}
		s.active = seg
		state.inserted = created
		s.evictLocked(hour)
	}
	state.seg = s.active
	state.bufLen = len(s.active.buf)
	state.count = s.active.count

	raw := Encode(ts, values)
	s.active.buf = append(s.active.buf, raw...)
	s.active.count++
	s.seq++
	rec := Record{Seq: s.seq, UnixNano: ts, Values: values}
	s.latest = rec
	s.hasLatest = true
	s.lastTs = ts
	s.hasLast = true
	if s.active.file == nil {
		s.undoLocked(state)
		return prepared{}, Record{}, errors.New("小时文件未打开")
	}
	return prepared{file: s.active.file, raw: raw, state: state}, rec, nil
}

func (s *Store) persist(p prepared, doSync bool) error {
	off, err := p.file.Seek(0, io.SeekEnd)
	if err != nil {
		s.undo(p.state)
		return err
	}
	n, err := p.file.Write(p.raw)
	if err != nil || n != len(p.raw) {
		_ = p.file.Truncate(off)
		_, _ = p.file.Seek(off, io.SeekStart)
		s.undo(p.state)
		if err == nil {
			err = io.ErrShortWrite
		}
		return err
	}
	if !doSync {
		return nil
	}
	if s.opts.BeforeSync != nil {
		s.opts.BeforeSync()
	}
	if s.opts.DisableSync {
		return nil
	}
	return p.file.Sync()
}

func (s *Store) undo(st memState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.undoLocked(st)
}

func (s *Store) undoLocked(st memState) {
	if st.seg != nil && st.seg.buf != nil {
		st.seg.buf = st.seg.buf[:st.bufLen]
		st.seg.count = st.count
	}
	s.lastTs = st.lastTs
	s.hasLast = st.hasLast
	s.latest = st.latest
	s.hasLatest = st.hasLatest
	s.seq = st.seq
	if !st.inserted || st.seg == nil {
		return
	}
	if st.seg.file != nil {
		_ = st.seg.file.Close()
		st.seg.file = nil
	}
	_ = os.Remove(st.seg.path)
	s.segs = removeSeg(s.segs, st.seg)
	if s.active == st.seg {
		s.active = st.prevActive
	}
}

func (s *Store) openHourLocked(hour int64) (*segment, bool, error) {
	if seg := s.find(hour); seg != nil {
		if err := s.activate(seg); err != nil {
			return nil, false, err
		}
		return seg, false, nil
	}
	path := segmentPath(s.opts.DataDir, hour)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, false, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, false, err
	}
	if _, err := f.Write(encodeHeader(s.headerFor(hour))); err != nil {
		f.Close()
		_ = os.Remove(path)
		return nil, false, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(path)
		return nil, false, err
	}
	seg := &segment{hourStart: hour, path: path, file: f, buf: []byte{}}
	s.segs = insertSeg(s.segs, seg)
	return seg, true, nil
}

func (s *Store) activate(seg *segment) error {
	if seg.file != nil {
		return nil
	}
	f, err := os.OpenFile(seg.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if seg.buf == nil {
		payload, err := readPayload(f)
		if err != nil {
			f.Close()
			return err
		}
		seg.buf = payload
		seg.count = len(payload) / s.recordSize
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return err
	}
	seg.file = f
	return nil
}

func (s *Store) evictLocked(newest int64) {
	minHour := newest - int64(s.opts.CacheHours-1)*int64(time.Hour)
	for _, seg := range s.segs {
		if seg.hourStart < minHour && seg != s.active {
			seg.buf = nil
		}
	}
}

func (s *Store) find(hour int64) *segment {
	for _, seg := range s.segs {
		if seg.hourStart == hour {
			return seg
		}
	}
	return nil
}

func (s *Store) segmentBytes(seg *segment) ([]byte, error) {
	if seg.buf != nil {
		return seg.buf, nil
	}
	f, err := os.Open(seg.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readPayload(f)
}

func (s *Store) load() error {
	days, err := os.ReadDir(s.opts.DataDir)
	if err != nil {
		return err
	}
	var segs []*segment
	for _, day := range days {
		if !day.IsDir() {
			continue
		}
		if _, err := time.Parse("2006-01-02", day.Name()); err != nil {
			continue
		}
		hours, err := os.ReadDir(filepath.Join(s.opts.DataDir, day.Name()))
		if err != nil {
			return err
		}
		for _, h := range hours {
			if h.IsDir() || len(h.Name()) != len("15.seg") || h.Name()[2:] != ".seg" {
				continue
			}
			path := filepath.Join(s.opts.DataDir, day.Name(), h.Name())
			seg, err := s.loadSegment(path, day.Name(), h.Name())
			if err != nil {
				return err
			}
			if seg != nil {
				segs = append(segs, seg)
			}
		}
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].hourStart < segs[j].hourStart })
	s.segs = segs
	if len(segs) == 0 {
		return nil
	}
	newest := segs[len(segs)-1].hourStart
	minHour := newest - int64(s.opts.CacheHours-1)*int64(time.Hour)
	var last Record
	found := false
	for _, seg := range segs {
		if seg.hourStart >= minHour {
			if err := s.fillCache(seg); err != nil {
				return err
			}
		}
		if seg.count == 0 {
			continue
		}
		rec, err := s.lastRecord(seg)
		if err != nil {
			return err
		}
		if !found || rec.UnixNano >= last.UnixNano {
			last = rec
			found = true
		}
	}
	if found {
		s.latest = last
		s.hasLatest = true
		s.lastTs = last.UnixNano
		s.hasLast = true
	}
	return nil
}

func (s *Store) loadSegment(path, day, name string) (*segment, error) {
	hour, err := hourFromNames(day, name)
	if err != nil {
		return nil, fmt.Errorf("段文件名无效 %s: %w", path, err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	seg, removeFile, err := s.inspectSegment(f, path, hour)
	closeErr := f.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if removeFile {
		if err := os.Remove(path); err != nil {
			return nil, err
		}
		log.Printf("删除不完整段文件 %s", path)
		return nil, nil
	}
	return seg, nil
}

func (s *Store) inspectSegment(f *os.File, path string, hour int64) (*segment, bool, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, false, err
	}
	if st.Size() < HeaderSize {
		return nil, true, nil
	}
	hdr, err := readHeader(f)
	if err != nil {
		return nil, false, err
	}
	if string(hdr.Magic[:]) != Magic {
		return nil, false, fmt.Errorf("无法识别的段文件: %s", path)
	}
	if hdr.Version != Version {
		return nil, false, fmt.Errorf("不支持的段文件版本 %d: %s", hdr.Version, path)
	}
	if hdr.SchemaHash != s.hash || int(hdr.FieldCount) != s.fieldCount || int(hdr.RecordSize) != s.recordSize {
		return nil, false, fmt.Errorf("%w: %s", ErrSchemaMismatch, path)
	}
	if hdr.HourStart != hour {
		return nil, false, fmt.Errorf("段文件小时与路径不一致: %s", path)
	}
	payload := st.Size() - HeaderSize
	rem := payload % int64(s.recordSize)
	if rem != 0 {
		newSize := st.Size() - rem
		if err := f.Truncate(newSize); err != nil {
			return nil, false, err
		}
		if err := f.Sync(); err != nil {
			return nil, false, err
		}
		log.Printf("截断残缺尾部 %s，去掉 %d 字节", path, rem)
		payload -= rem
	}
	return &segment{
		hourStart: hour,
		path:      path,
		count:     int(payload / int64(s.recordSize)),
	}, false, nil
}

func (s *Store) fillCache(seg *segment) error {
	f, err := os.Open(seg.path)
	if err != nil {
		return err
	}
	defer f.Close()
	buf, err := readPayload(f)
	if err != nil {
		return err
	}
	seg.buf = buf
	seg.count = len(buf) / s.recordSize
	return nil
}

func (s *Store) lastRecord(seg *segment) (Record, error) {
	if seg.count <= 0 {
		return Record{}, io.EOF
	}
	if seg.buf != nil {
		off := (seg.count - 1) * s.recordSize
		return decodeRecord(seg.buf[off:off+s.recordSize], s.fieldCount), nil
	}
	f, err := os.Open(seg.path)
	if err != nil {
		return Record{}, err
	}
	defer f.Close()
	buf := make([]byte, s.recordSize)
	off := int64(HeaderSize) + int64(seg.count-1)*int64(s.recordSize)
	if _, err := f.ReadAt(buf, off); err != nil {
		return Record{}, err
	}
	return decodeRecord(buf, s.fieldCount), nil
}

func (s *Store) recomputeLastLocked() {
	s.hasLast = false
	s.hasLatest = false
	s.latest = Record{}
	s.lastTs = 0
	for i := len(s.segs) - 1; i >= 0; i-- {
		if s.segs[i].count == 0 {
			continue
		}
		rec, err := s.lastRecord(s.segs[i])
		if err != nil {
			log.Printf("重算最新样本失败: %v", err)
			return
		}
		s.latest = rec
		s.hasLatest = true
		s.lastTs = rec.UnixNano
		s.hasLast = true
		return
	}
}

func readPayload(f *os.File) ([]byte, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	n := st.Size() - HeaderSize
	if n <= 0 {
		return []byte{}, nil
	}
	buf := make([]byte, n)
	_, err = f.ReadAt(buf, HeaderSize)
	return buf, err
}

func binaryUint64(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

func cloneRecord(rec Record) Record {
	rec.Values = append([]float64(nil), rec.Values...)
	return rec
}

func insertSeg(segs []*segment, seg *segment) []*segment {
	i := sort.Search(len(segs), func(i int) bool { return segs[i].hourStart >= seg.hourStart })
	segs = append(segs, nil)
	copy(segs[i+1:], segs[i:])
	segs[i] = seg
	return segs
}

func removeSeg(segs []*segment, target *segment) []*segment {
	out := make([]*segment, 0, len(segs))
	for _, seg := range segs {
		if seg != target {
			out = append(out, seg)
		}
	}
	return out
}

// cached 报告这个小时的原始字节是否在内存里，只给测试用。
func (s *Store) cached(hour int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seg := s.find(hour)
	return seg != nil && seg.buf != nil
}
