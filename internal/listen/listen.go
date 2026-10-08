// Package listen 绑定端口。配置的端口被占用时，向后尝试后续端口。
package listen

import (
	"errors"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"syscall"
)

const maxTries = 40

// Bound 是一次绑定的结果。Cause 非空表示没有用成请求的地址。
type Bound struct {
	Listener  net.Listener
	Addr      string
	Requested string
	Cause     error
}

// Listen 绑定 address。skip 里的端口会跳过，成功后把实际端口记入 skip。
func Listen(network, address string, skip map[int]bool) (Bound, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return Bound{}, fmt.Errorf("地址 %s: %w", address, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 0 || port > 65535 {
		return Bound{}, fmt.Errorf("端口无效: %s", address)
	}
	if port == 0 {
		ln, err := net.Listen(network, address)
		if err != nil {
			return Bound{}, err
		}
		return Bound{Listener: ln, Addr: ln.Addr().String(), Requested: address}, nil
	}

	var first error
	tried := 0
	for p := port; p <= 65535 && tried < maxTries; p++ {
		if skip[p] {
			continue
		}
		tried++
		try := net.JoinHostPort(host, strconv.Itoa(p))
		ln, err := net.Listen(network, try)
		if err == nil {
			if skip != nil {
				skip[p] = true
			}
			bound := Bound{Listener: ln, Addr: ln.Addr().String(), Requested: address}
			if p != port {
				bound.Cause = first
			}
			return bound, nil
		}
		if first == nil {
			first = err
		}
		if !busy(err) {
			return Bound{}, err
		}
	}
	return Bound{}, fmt.Errorf("从 %s 起没有可用端口: %w", address, first)
}

func busy(err error) bool {
	if errors.Is(err, syscall.EADDRINUSE) {
		return true
	}
	// Windows 把保留端口也报成“权限不允许”，netstat 里看不到占用进程。
	return runtime.GOOS == "windows" && errors.Is(err, syscall.EACCES)
}
