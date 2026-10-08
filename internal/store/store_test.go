package store

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"tsdb/internal/schema"
)

func openAt(t *testing.T, dir string, mutate func(*Options)) *Store {
	t.Helper()
	opts := Options{
		DataDir:    dir,
		Fields:     []string{"a", "b", "c"},
		Retention:  14 * 24 * time.Hour,
		CacheHours: 3,
	}
	if mutate != nil {
		mutate(&opts)
	}
	st, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func openStore(t *testing.T, mutate func(*Options)) *Store {
	t.Helper()
	return openAt(t, t.TempDir(), mutate)
}

func TestRoundTripAndBounds(t *testing.T) {
	st := openStore(t, nil)
	base := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	points := []int64{0, 10, 20}
	for _, sec := range points {
		vals := []float64{float64(sec), 2, 3}
		if err := st.Append(base.Add(time.Duration(sec)*time.Second).UnixNano(), vals); err != nil {
			t.Fatal(err)
		}
	}
	// 调用方之后改写切片，不能影响已写入的样本。
	vals := []float64{1, 2, 3}
	if err := st.Append(base.Add(30*time.Second).UnixNano(), vals); err != nil {
		t.Fatal(err)
	}
	vals[0] = 99

	from := base.Add(10 * time.Second).UnixNano()
	to := base.Add(20 * time.Second).UnixNano()
	got, err := st.Query(from, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Values[0] != 10 {
		t.Fatalf("半开区间得到 %+v", got)
	}
	got, err = st.Query(from, base.Add(21*time.Second).UnixNano())
	if err != nil || len(got) != 2 {
		t.Fatalf("len=%d err=%v", len(got), err)
	}
	got, err = st.Query(base.UnixNano(), base.UnixNano())
	if err != nil || len(got) != 0 {
		t.Fatalf("空区间 len=%d", len(got))
	}

	path := filepath.Join(st.opts.DataDir, "2026-10-08", "12.seg")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw[:4]) != Magic {
		t.Fatalf("魔数 %q", raw[:4])
	}
	payload := raw[HeaderSize:]
	if len(payload)%schema.RecordSize(3) != 0 {
		t.Fatalf("长度 %d", len(payload))
	}
}

func TestSameSecondAndNaN(t *testing.T) {
	st := openStore(t, nil)
	ts := time.Date(2026, 10, 8, 12, 1, 2, 0, time.UTC).UnixNano()
	if err := st.Append(ts, []float64{1, math.NaN(), 3}); err != nil {
		t.Fatal(err)
	}
	if err := st.Append(ts, []float64{4, 5, 6}); err != nil {
		t.Fatal(err)
	}
	if err := st.Append(ts-1, []float64{0, 0, 0}); !errors.Is(err, ErrTimeRegression) {
		t.Fatalf("倒退: %v", err)
	}
	got, err := st.Query(ts, ts+1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !math.IsNaN(got[0].Values[1]) || got[1].Values[0] != 4 {
		t.Fatalf("%+v", got)
	}
	stats := st.Stats()
	if stats.Records != 2 {
		t.Fatalf("records %d", stats.Records)
	}
}

func TestLatestAcrossHour(t *testing.T) {
	st := openStore(t, nil)
	t1 := time.Date(2026, 10, 8, 12, 59, 59, 0, time.UTC)
	t2 := time.Date(2026, 10, 8, 13, 0, 0, 0, time.UTC)
	if err := st.Append(t1.UnixNano(), []float64{1, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := st.Append(t2.UnixNano(), []float64{2, 0, 0}); err != nil {
		t.Fatal(err)
	}
	got, err := st.Latest(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].UnixNano != t1.UnixNano() || got[1].UnixNano != t2.UnixNano() {
		t.Fatalf("%+v", got)
	}
	one, err := st.Latest(1)
	if err != nil || len(one) != 1 || one[0].Values[0] != 2 {
		t.Fatalf("%+v %v", one, err)
	}
}

func TestReopenAndTornTail(t *testing.T) {
	dir := t.TempDir()
	opts := Options{DataDir: dir, Fields: []string{"a", "b", "c"}, CacheHours: 3}
	st, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2026, 10, 8, 8, 0, 0, 0, time.UTC).UnixNano()
	if err := st.Append(ts, []float64{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := st.Append(ts+int64(time.Second), []float64{4, 5, 6}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "2026-10-08", "08.seg")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{9, 9, 9, 9, 9}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	st, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	got, err := st.Query(ts, ts+int64(2*time.Second))
	if err != nil || len(got) != 2 || got[1].Values[0] != 4 {
		t.Fatalf("%+v %v", got, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	want := int64(HeaderSize + 2*schema.RecordSize(3))
	if info.Size() != want {
		t.Fatalf("size %d want %d", info.Size(), want)
	}
	if err := st.Append(ts+int64(2*time.Second), []float64{7, 8, 9}); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaMismatch(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(Options{DataDir: dir, Fields: []string{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2026, 5, 1, 0, 0, 1, 0, time.UTC).UnixNano()
	if err := st.Append(ts, []float64{1, 2}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = Open(Options{DataDir: dir, Fields: []string{"a", "c"}})
	if !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("got %v", err)
	}
}

func TestRetention(t *testing.T) {
	dir := t.TempDir()
	st := openAt(t, dir, func(o *Options) {
		o.Retention = time.Hour
	})
	old := time.Date(2020, 1, 2, 3, 4, 0, 0, time.UTC)
	fresh := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	if err := st.Append(old.UnixNano(), []float64{1, 1, 1}); err != nil {
		t.Fatal(err)
	}
	if err := st.Append(fresh.UnixNano(), []float64{2, 2, 2}); err != nil {
		t.Fatal(err)
	}
	n, err := st.RunRetention(fresh)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("deleted %d", n)
	}
	if _, err := os.Stat(filepath.Join(dir, "2020-01-02")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("旧目录还在: %v", err)
	}
	got, err := st.Query(old.UnixNano(), fresh.Add(time.Second).UnixNano())
	if err != nil || len(got) != 1 || got[0].Values[0] != 2 {
		t.Fatalf("%+v %v", got, err)
	}
	if _, ok := st.Last(); !ok {
		t.Fatal("最新样本应该还在")
	}
}

func TestColdHourStillReadable(t *testing.T) {
	st := openStore(t, func(o *Options) { o.CacheHours = 1 })
	h0 := time.Date(2026, 10, 8, 10, 0, 0, 0, time.UTC)
	h1 := h0.Add(time.Hour)
	if err := st.Append(h0.UnixNano(), []float64{1, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := st.Append(h1.UnixNano(), []float64{2, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if st.cached(h0.UnixNano()) {
		t.Fatal("超出缓存窗口的小时不应留在内存")
	}
	if !st.cached(h1.UnixNano()) {
		t.Fatal("当前小时应该在内存里")
	}
	got, err := st.Query(h0.UnixNano(), h1.UnixNano()+1)
	if err != nil || len(got) != 2 || got[0].Values[0] != 1 || got[1].Values[0] != 2 {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestDirectoryLock(t *testing.T) {
	dir := t.TempDir()
	first, err := Open(Options{DataDir: dir, Fields: []string{"a"}})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	_, err = Open(Options{DataDir: dir, Fields: []string{"a"}})
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("got %v", err)
	}
}

func TestVisibleBeforeSync(t *testing.T) {
	notified := make(chan Record, 1)
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	defer free()

	st := openStore(t, func(o *Options) {
		o.OnSample = func(rec Record) { notified <- rec }
		o.BeforeSync = func() {
			close(entered)
			<-release
		}
	})
	ts := time.Date(2026, 10, 8, 15, 0, 0, 0, time.UTC).UnixNano()
	errc := make(chan error, 1)
	go func() {
		errc <- st.Append(ts, []float64{8, 0, 0})
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("没有走到刷盘")
	}
	select {
	case rec := <-notified:
		if rec.Values[0] != 8 || rec.Seq == 0 {
			t.Fatalf("%+v", rec)
		}
	default:
		t.Fatal("刷盘前没有通知订阅者")
	}
	got, err := st.Query(ts, ts+1)
	if err != nil || len(got) != 1 {
		t.Fatalf("内存里还看不到样本: %+v %v", got, err)
	}
	select {
	case err := <-errc:
		t.Fatalf("写入在刷盘前返回了: %v", err)
	default:
	}
	free()
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestBatch(t *testing.T) {
	var got []Record
	st := openStore(t, func(o *Options) {
		o.DisableSync = true
		o.OnSample = func(rec Record) { got = append(got, cloneRecord(rec)) }
	})
	ts := time.Date(2026, 10, 8, 16, 0, 0, 0, time.UTC).UnixNano()
	err := st.AppendMany([]Record{
		{UnixNano: ts, Values: []float64{1, 0, 0}},
		{UnixNano: ts - 1, Values: []float64{2, 0, 0}},
	})
	if !errors.Is(err, ErrTimeRegression) {
		t.Fatalf("got %v", err)
	}
	if n := st.Stats().Records; n != 0 {
		t.Fatalf("校验失败仍然写入了 %d 条", n)
	}
	err = st.AppendMany([]Record{
		{UnixNano: ts, Values: []float64{1, 0, 0}},
		{UnixNano: ts, Values: []float64{2, 0, 0}},
		{UnixNano: ts + 5, Values: []float64{3, 0, 0}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Values[0] != 3 {
		t.Fatalf("批量应该只推最后一条: %+v", got)
	}
	recs, err := st.Query(ts, ts+6)
	if err != nil || len(recs) != 3 {
		t.Fatalf("%+v %v", recs, err)
	}
}

func TestConcurrentReaders(t *testing.T) {
	st := openStore(t, func(o *Options) { o.DisableSync = true })
	start := time.Date(2026, 10, 8, 18, 0, 0, 0, time.UTC).UnixNano()
	const n = 200
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stop)
		vals := []float64{0, 1, 2}
		for i := 0; i < n; i++ {
			vals[0] = float64(i)
			if err := st.Append(start+int64(i)*int64(time.Millisecond), append([]float64(nil), vals...)); err != nil {
				t.Errorf("写入: %v", err)
				return
			}
		}
	}()
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if _, err := st.Latest(5); err != nil {
					t.Errorf("最新: %v", err)
					return
				}
				select {
				case <-stop:
					got, err := st.Query(start, start+int64(n)*int64(time.Millisecond))
					if err != nil {
						t.Errorf("查询: %v", err)
						return
					}
					if len(got) != n {
						t.Errorf("读到 %d 条，想要 %d", len(got), n)
					}
					return
				default:
				}
			}
		}()
	}
	wg.Wait()
}
