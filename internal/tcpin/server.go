package tcpin

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"time"

	"tsdb/internal/store"
)

const (
	// MaxLineBytes 是单行报文上限。一行只放一个识别码和一个浮点。
	MaxLineBytes = 4096
	// MaxBuffered 是一条连接上尚未换行的读取缓冲。
	// 一秒的文本一般不超过 40960 位（5120 字节），这里留到 64KiB，不再卡在 1024。
	MaxBuffered = 64 * 1024
)

// Server 在局域网上接收传感器中枢的 TCP 报文。它不主动访问公网。
type Server struct {
	Addr  string
	Store *store.Store
	Codes []string
	Idle  time.Duration
	// Notify 在开始接受连接后收到实际监听地址，测试用来拿到系统分配的端口。
	Notify chan<- string
}

// Serve 在 Addr 上接受连接，直到 ctx 结束。
func (s *Server) Serve(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	return s.ServeListener(ctx, ln)
}

// ServeListener 使用已经绑定好的监听器。
func (s *Server) ServeListener(ctx context.Context, ln net.Listener) error {
	defer ln.Close()
	if s.Notify != nil {
		s.Notify <- ln.Addr().String()
	}
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go s.handle(ctx, conn)
	}
}

func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	asm := NewAssembler(s.Codes)
	idle := s.Idle
	if idle <= 0 {
		idle = 200 * time.Millisecond
	}
	reader := bufio.NewReaderSize(conn, 16*1024)
	lines := make(chan string, 32)
	errc := make(chan error, 1)
	go func() {
		for {
			line, err := readLine(reader, MaxLineBytes)
			if line != "" {
				select {
				case lines <- line:
				case <-ctx.Done():
					return
				}
			}
			if err != nil {
				errc <- err
				return
			}
		}
	}()
	timer := time.NewTimer(idle)
	defer timer.Stop()
	commit := func(values []float64) {
		if err := s.Store.Append(time.Now().UnixNano(), values); err != nil {
			log.Printf("写入传感器帧: %v", err)
		}
	}
	for {
		select {
		case <-ctx.Done():
			if values, ok := asm.Flush(); ok {
				commit(values)
			}
			return
		case line := <-lines:
			if values, ok := asm.FeedLine(line); ok {
				commit(values)
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(idle)
		case <-timer.C:
			if values, ok := asm.Flush(); ok {
				commit(values)
			}
			timer.Reset(idle)
		case err := <-errc:
			// 行先进队列，错误后到。先把已经读到的行吃完，再收口最后一帧。
			for {
				select {
				case line := <-lines:
					if values, ok := asm.FeedLine(line); ok {
						commit(values)
					}
				default:
					if values, ok := asm.Flush(); ok {
						commit(values)
					}
					if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
						log.Printf("传感器连接 %s: %v", conn.RemoteAddr(), err)
					}
					return
				}
			}
		}
	}
}

func readLine(r *bufio.Reader, max int) (string, error) {
	var buf []byte
	for {
		part, err := r.ReadSlice('\n')
		if len(buf)+len(part) > max {
			return "", errors.New("报文行超过长度上限")
		}
		buf = append(buf, part...)
		if err == bufio.ErrBufferFull {
			continue
		}
		return string(buf), err
	}
}
