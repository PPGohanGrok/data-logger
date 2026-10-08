package listen

import (
	"net"
	"strconv"
	"testing"
)

func TestUsesRequestedPort(t *testing.T) {
	holder, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	holder.Close()
	_, portText, _ := net.SplitHostPort(holder.Addr().String())
	addr := "127.0.0.1:" + portText

	bound, err := Listen("tcp", addr, map[int]bool{})
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Listener.Close()
	if bound.Cause != nil {
		t.Fatalf("空端口不应改道: %v", bound.Cause)
	}
	if bound.Addr != addr && bound.Addr != holder.Addr().String() {
		// Listen may normalize the address; compare ports.
		_, got, _ := net.SplitHostPort(bound.Addr)
		if got != portText {
			t.Fatalf("got %s want %s", bound.Addr, addr)
		}
	}
}

func TestSkipsBusyPort(t *testing.T) {
	holder, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	_, portText, _ := net.SplitHostPort(holder.Addr().String())
	port, _ := strconv.Atoi(portText)

	bound, err := Listen("tcp", holder.Addr().String(), map[int]bool{})
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Listener.Close()
	if bound.Cause == nil {
		t.Fatal("占用的端口应该改道")
	}
	_, gotText, _ := net.SplitHostPort(bound.Addr)
	got, _ := strconv.Atoi(gotText)
	if got == port {
		t.Fatalf("仍绑定在 %d", got)
	}
}
