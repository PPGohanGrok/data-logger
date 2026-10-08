// Package live 把新样本推给实时订阅者。
// 队列只有几条；排满后丢掉中间样本，只保留最新一条，因此慢客户端不会堵住写入。
package live

import "sync"

// 大约覆盖每秒数条的短时抖动。正常 1 Hz 画面来得及读走每一条。
const queueSize = 4

// Sample 是推给前端的一条样本。
type Sample struct {
	Seq      uint64
	UnixNano int64
	Values   []float64
}

// Hub 是进程内的订阅表。
type Hub struct {
	mu   sync.Mutex
	next uint64
	subs map[uint64]*Sub
}

// Sub 是一个实时客户端。
type Sub struct {
	id     uint64
	C      chan Sample
	hub    *Hub
	mu     sync.Mutex
	closed bool
}

// New 创建一个没有订阅者的 Hub。
func New() *Hub {
	return &Hub{subs: make(map[uint64]*Sub)}
}

// Publish 非阻塞地把样本交给每个订阅者。Values 会被复制。
func (h *Hub) Publish(sample Sample) {
	sample.Values = append([]float64(nil), sample.Values...)
	h.mu.Lock()
	subs := make([]*Sub, 0, len(h.subs))
	for _, sub := range h.subs {
		subs = append(subs, sub)
	}
	h.mu.Unlock()
	for _, sub := range subs {
		sub.push(sample)
	}
}

// Subscribe 登记一个新客户端。用完必须 Close。
func (h *Hub) Subscribe() *Sub {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.next++
	sub := &Sub{
		id:  h.next,
		C:   make(chan Sample, queueSize),
		hub: h,
	}
	h.subs[sub.id] = sub
	return sub
}

// Close 取消订阅。之后 Publish 不再碰到这个客户端。
func (s *Sub) Close() {
	s.hub.mu.Lock()
	delete(s.hub.subs, s.id)
	s.hub.mu.Unlock()
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

func (s *Sub) push(sample Sample) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.C <- sample:
		return
	default:
	}
	for {
		select {
		case <-s.C:
		default:
			goto send
		}
	}
send:
	select {
	case s.C <- sample:
	default:
	}
}
