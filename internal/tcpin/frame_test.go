package tcpin

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestClassicPacket(t *testing.T) {
	asm := NewAssembler([]string{"0108", "0110"})
	text := "&&\r\n010812.5\r\n01103\r\n!!\r\n"
	var got []float64
	for _, line := range strings.Split(text, "\n") {
		if frame, ok := asm.FeedLine(line); ok {
			got = frame
		}
	}
	if len(got) != 2 || got[0] != 12.5 || got[1] != 3 || math.IsNaN(got[0]) {
		t.Fatalf("%v", got)
	}
}

func TestInlineMarkerAndRepeat(t *testing.T) {
	asm := NewAssembler([]string{"0108", "0110"})
	lines := []string{"&&01081.5", "&&01102.25", "&&01089"}
	var closed []float64
	for _, line := range lines {
		if frame, ok := asm.FeedLine(line); ok {
			closed = frame
		}
	}
	if len(closed) != 2 || closed[0] != 1.5 || closed[1] != 2.25 {
		t.Fatalf("closed %v", closed)
	}
	rest, ok := asm.Flush()
	if !ok || rest[0] != 9 || !math.IsNaN(rest[1]) {
		t.Fatalf("rest %v %v", rest, ok)
	}
}

func TestUnknownCodeSkipped(t *testing.T) {
	asm := NewAssembler([]string{"0108"})
	if _, ok := asm.FeedLine("&&99991.0"); ok {
		t.Fatal("未知识别码不该收口")
	}
	if _, ok := asm.FeedLine("not a line"); ok {
		t.Fatal("坏行不该收口")
	}
	frame, ok := asm.FeedLine("!!")
	if ok || frame != nil {
		t.Fatal("空帧不该写入")
	}
	asm.FeedLine("&&0108-2.5")
	frame, ok = asm.Flush()
	if !ok || frame[0] != -2.5 {
		t.Fatalf("%v", frame)
	}
}

func TestFrameLargerThan1024Bytes(t *testing.T) {
	const n = 160
	codes := make([]string, n)
	var b strings.Builder
	b.WriteString("&&\n")
	for i := 0; i < n; i++ {
		codes[i] = fmt.Sprintf("%04d", i+1)
		fmt.Fprintf(&b, "%s1.5\n", codes[i])
	}
	b.WriteString("!!\n")
	if b.Len() <= 1024 {
		t.Fatalf("样例只有 %d 字节", b.Len())
	}
	asm := NewAssembler(codes)
	var got []float64
	for _, line := range strings.Split(b.String(), "\n") {
		if frame, ok := asm.FeedLine(line); ok {
			got = frame
		}
	}
	if len(got) != n || got[n-1] != 1.5 {
		t.Fatalf("len %d", len(got))
	}
}
