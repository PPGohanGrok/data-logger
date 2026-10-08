package tcpin

import (
	"math"
	"strconv"
	"strings"
	"unicode"
)

// Assembler 把一帧传感器报文收成一条等宽样本。
// 识别码不在配置里的行会被跳过。缺的字段保持 NaN。
type Assembler struct {
	index map[string]int
	width int
	vals  []float64
	seen  map[string]bool
	dirty bool
}

func NewAssembler(codes []string) *Assembler {
	index := make(map[string]int, len(codes))
	for i, code := range codes {
		if code != "" {
			index[code] = i
		}
	}
	a := &Assembler{index: index, width: len(codes)}
	a.reset()
	return a
}

// FeedLine 喂一行 UTF-8 文本。
// 一帧在单独的 "&&"、结束符 "!!"，或同一个识别码再次出现时收口。
// "&&0108123.4" 这种同一行的写法也接受。
func (a *Assembler) FeedLine(line string) ([]float64, bool) {
	line = strings.TrimSpace(strings.TrimPrefix(line, "\uFEFF"))
	if line == "" {
		return nil, false
	}
	if line == "&&" {
		return a.Flush()
	}
	if line == "!!" {
		return a.Flush()
	}
	payload := line
	if strings.HasPrefix(payload, "&&") {
		payload = strings.TrimSpace(payload[2:])
	}
	if len(payload) < 4 || !digits(payload[:4]) {
		return nil, false
	}
	code := payload[:4]
	idx, ok := a.index[code]
	if !ok {
		return nil, false
	}
	raw := strings.TrimSpace(payload[4:])
	if raw == "" {
		return nil, false
	}
	if raw[0] == '+' {
		raw = raw[1:]
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil, false
	}
	var closed []float64
	closedOK := false
	if a.seen[code] {
		closed, closedOK = a.Flush()
	}
	a.vals[idx] = value
	a.seen[code] = true
	a.dirty = true
	return closed, closedOK
}

// Flush 交出当前帧。没有数据时不交出。
func (a *Assembler) Flush() ([]float64, bool) {
	if !a.dirty {
		return nil, false
	}
	out := append([]float64(nil), a.vals...)
	a.reset()
	return out, true
}

func (a *Assembler) reset() {
	a.vals = make([]float64, a.width)
	for i := range a.vals {
		a.vals[i] = math.NaN()
	}
	a.seen = make(map[string]bool, len(a.index))
	a.dirty = false
}

func digits(s string) bool {
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}
