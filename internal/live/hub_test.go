package live

import (
	"testing"
	"time"
)

func TestPublishReachesKeeper(t *testing.T) {
	h := New()
	sub := h.Subscribe()
	defer sub.Close()
	h.Publish(Sample{Seq: 1, UnixNano: 10, Values: []float64{1.5}})
	got := <-sub.C
	if got.Seq != 1 || got.Values[0] != 1.5 {
		t.Fatalf("%+v", got)
	}
	got.Values[0] = 9
	h.Publish(Sample{Seq: 2, UnixNano: 11, Values: []float64{1.5}})
	got = <-sub.C
	if got.Values[0] != 1.5 {
		t.Fatal("推送不应该和调用方共享底层数组")
	}
}

func TestSlowClientSeesLatest(t *testing.T) {
	h := New()
	sub := h.Subscribe()
	defer sub.Close()
	start := time.Now()
	const n = 1000
	for i := 1; i <= n; i++ {
		h.Publish(Sample{Seq: uint64(i), UnixNano: int64(i)})
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatalf("慢客户端堵住了写入，耗时 %s", time.Since(start))
	}
	var got []Sample
	for {
		select {
		case sample := <-sub.C:
			got = append(got, sample)
		default:
			goto done
		}
	}
done:
	if len(got) == 0 || len(got) > queueSize {
		t.Fatalf("积压了 %d 条，队列容量是 %d", len(got), queueSize)
	}
	if got[len(got)-1].Seq != n {
		t.Fatalf("恢复后最后一条是 %d，想要 %d", got[len(got)-1].Seq, n)
	}
}

func TestClosedSubDrops(t *testing.T) {
	h := New()
	sub := h.Subscribe()
	sub.Close()
	h.Publish(Sample{Seq: 1})
	select {
	case <-sub.C:
		t.Fatal("关闭后不应该再收到样本")
	default:
	}
}
