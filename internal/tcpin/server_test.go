package tcpin

import (
	"context"
	"math"
	"net"
	"testing"
	"time"

	"tsdb/internal/store"
)

func TestTCPWritesFrame(t *testing.T) {
	st, err := store.Open(store.Options{
		DataDir:     t.TempDir(),
		Fields:      []string{"井深", "钻头深度"},
		CacheHours:  1,
		DisableSync: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	srv := &Server{
		Addr:   "127.0.0.1:0",
		Store:  st,
		Codes:  []string{"0108", "0110"},
		Idle:   time.Hour,
		Notify: ready,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ctx) }()
	addr := <-ready

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("&&\n01081234.5\n011080.25\n!!\n")); err != nil {
		t.Fatal(err)
	}
	conn.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		got, err := st.Latest(1)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) == 1 {
			if got[0].Values[0] != 1234.5 || got[0].Values[1] != 80.25 {
				t.Fatalf("%v", got[0].Values)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("没有等到这一帧")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("TCP 服务没有退出")
	}
}

func TestTCPIdleFlush(t *testing.T) {
	st, err := store.Open(store.Options{
		DataDir:     t.TempDir(),
		Fields:      []string{"井深", "钻头深度"},
		CacheHours:  1,
		DisableSync: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	srv := &Server{
		Addr:   "127.0.0.1:0",
		Store:  st,
		Codes:  []string{"0108", "0110"},
		Idle:   40 * time.Millisecond,
		Notify: ready,
	}
	go srv.Serve(ctx)
	conn, err := net.Dial("tcp", <-ready)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("&&01086.5\n")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		got, err := st.Latest(1)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) == 1 {
			if got[0].Values[0] != 6.5 || !math.IsNaN(got[0].Values[1]) {
				t.Fatalf("%v", got[0].Values)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("停顿后没有写入")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
