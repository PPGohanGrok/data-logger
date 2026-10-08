package store

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkQueryTwoHours(b *testing.B) {
	fields := make([]string, 32)
	for i := range fields {
		fields[i] = fmt.Sprintf("ch%02d", i+1)
	}
	st, err := Open(Options{
		DataDir:     b.TempDir(),
		Fields:      fields,
		CacheHours:  3,
		DisableSync: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()

	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	values := make([]float64, len(fields))
	const points = 7200
	for i := 0; i < points; i++ {
		values[0] = float64(i)
		if err := st.Append(base+int64(i)*int64(time.Second), values); err != nil {
			b.Fatal(err)
		}
	}
	from := base
	to := base + points*int64(time.Second)
	if !st.cached(hourStart(from)) || !st.cached(hourStart(from+int64(time.Hour))) {
		b.Fatal("两小时数据应该都在热缓存里")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := st.Query(from, to)
		if err != nil || len(got) != points {
			b.Fatalf("len=%d err=%v", len(got), err)
		}
	}
}
