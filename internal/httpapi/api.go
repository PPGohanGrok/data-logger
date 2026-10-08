// Package httpapi 提供本机写入、范围查询和实时推送。
package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"tsdb/internal/live"
	"tsdb/internal/schema"
	"tsdb/internal/store"
)

const maxBody = 16 << 20

const idleAfter = 2 * time.Second

// Server 把存储和实时订阅接到 HTTP。
type Server struct {
	Store         *store.Store
	Hub           *live.Hub
	Fields        []string
	Codes         []string
	RetentionDays int
	CacheHours    int
}

type wireRecord struct {
	TS     string     `json:"ts"`
	Values []*float64 `json:"values"`
}

type queryResponse struct {
	Records []wireRecord `json:"records"`
}

// Handler 是带跨域头的路由。页面在别的端口时也能读本机接口。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/write", s.handleWrite)
	mux.HandleFunc("POST /v1/write/batch", s.handleBatch)
	mux.HandleFunc("GET /v1/query", s.handleQuery)
	mux.HandleFunc("GET /v1/latest", s.handleLatest)
	mux.HandleFunc("GET /v1/live", s.handleLive)
	mux.HandleFunc("GET /v1/schema", s.handleSchema)
	mux.HandleFunc("GET /v1/health", s.handleHealth)
	mux.HandleFunc("GET /favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /{$}", s.handlePage)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	body := http.MaxBytesReader(w, r.Body, maxBody)
	defer body.Close()
	ts, values, err := s.readOne(body, r.Header.Get("Content-Type"))
	if err != nil {
		writeReadErr(w, err)
		return
	}
	if err := s.Store.Append(ts, values); err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	body := http.MaxBytesReader(w, r.Body, maxBody)
	defer body.Close()
	recs, err := readBatch(body)
	if err != nil {
		writeReadErr(w, err)
		return
	}
	if err := s.Store.AppendMany(recs); err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "count": len(recs)})
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	from, to, err := readRange(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	recs, err := s.Store.Query(from, to)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	format := strings.ToLower(r.URL.Query().Get("format"))
	if format == "" {
		format = "json"
	}
	switch format {
	case "json":
		writeJSON(w, http.StatusOK, queryResponse{Records: toWire(recs)})
	case "bin":
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Record-Size", strconv.Itoa(schema.RecordSize(len(s.Fields))))
		w.Header().Set("X-Field-Count", strconv.Itoa(len(s.Fields)))
		w.Header().Set("X-Count", strconv.Itoa(len(recs)))
		for _, rec := range recs {
			if _, err := w.Write(store.Encode(rec.UnixNano, rec.Values)); err != nil {
				return
			}
		}
	default:
		writeErr(w, http.StatusBadRequest, "format 只能是 json 或 bin")
	}
}

func (s *Server) handleLatest(w http.ResponseWriter, r *http.Request) {
	n := 1
	if raw := r.URL.Query().Get("n"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 {
			writeErr(w, http.StatusBadRequest, "n 必须是正整数")
			return
		}
		if v > 10000 {
			v = 10000
		}
		n = v
	}
	recs, err := s.Store.Latest(n)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, queryResponse{Records: toWire(recs)})
}

func (s *Server) handleLive(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "服务器不支持流式响应")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	sub := s.Hub.Subscribe()
	defer sub.Close()

	var seen uint64
	if last, ok := s.Store.Last(); ok {
		seen = last.Seq
		if err := writeSSE(w, "snapshot", marshalSample(last.UnixNano, last.Values)); err != nil {
			return
		}
	} else if err := writeSSE(w, "snapshot", []byte(`{"ts":"","values":null}`)); err != nil {
		return
	}
	flusher.Flush()

	idle := time.NewTimer(idleAfter)
	defer idle.Stop()
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-idle.C:
			payload, _ := json.Marshal(map[string]string{
				"ts": time.Now().UTC().Format(time.RFC3339Nano),
			})
			if err := writeSSE(w, "heartbeat", payload); err != nil {
				return
			}
			flusher.Flush()
			idle.Reset(idleAfter)
		case sample, ok := <-sub.C:
			if !ok {
				return
			}
			if sample.Seq != 0 && sample.Seq <= seen {
				continue
			}
			if sample.Seq != 0 {
				seen = sample.Seq
			}
			if err := writeSSE(w, "sample", marshalSample(sample.UnixNano, sample.Values)); err != nil {
				return
			}
			flusher.Flush()
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(idleAfter)
		}
	}
}

func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	body := map[string]any{
		"fields":         s.Fields,
		"field_count":    len(s.Fields),
		"record_size":    schema.RecordSize(len(s.Fields)),
		"schema_hash":    fmt.Sprintf("%016x", schema.Hash(s.Fields)),
		"retention_days": s.RetentionDays,
		"cache_hours":    s.CacheHours,
	}
	if len(s.Codes) == len(s.Fields) {
		body["codes"] = s.Codes
	}
	writeJSON(w, http.StatusOK, body)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	st := s.Store.Stats()
	body := map[string]any{
		"status":      "ok",
		"segments":    st.Segments,
		"records":     st.Records,
		"field_count": st.FieldCount,
	}
	if st.HasLast {
		body["last_ts"] = time.Unix(0, st.LastUnixNano).UTC().Format(time.RFC3339Nano)
	}
	writeJSON(w, http.StatusOK, body)
}

func (s *Server) pageData() struct{ Fields template.JS } {
	raw, err := json.Marshal(s.Fields)
	if err != nil {
		raw = []byte("[]")
	}
	return struct{ Fields template.JS }{template.JS(raw)}
}

func (s *Server) readOne(body io.Reader, contentType string) (int64, []float64, error) {
	if strings.HasPrefix(contentType, "application/octet-stream") {
		buf, err := io.ReadAll(body)
		if err != nil {
			return 0, nil, err
		}
		want := schema.RecordSize(len(s.Fields))
		if len(buf) != want {
			return 0, nil, fmt.Errorf("二进制记录长度应为 %d 字节", want)
		}
		rec, err := store.Decode(buf)
		if err != nil {
			return 0, nil, err
		}
		return rec.UnixNano, rec.Values, nil
	}
	return readJSONRecord(body)
}

func readJSONRecord(body io.Reader) (int64, []float64, error) {
	var raw struct {
		TS     json.RawMessage `json:"ts"`
		Values []*float64      `json:"values"`
	}
	dec := json.NewDecoder(body)
	if err := dec.Decode(&raw); err != nil {
		return 0, nil, fmt.Errorf("JSON 无效: %w", err)
	}
	if raw.Values == nil {
		return 0, nil, errors.New("缺少 values")
	}
	ts, err := parseTS(raw.TS)
	if err != nil {
		return 0, nil, err
	}
	return ts, floatValues(raw.Values), nil
}

func readBatch(body io.Reader) ([]store.Record, error) {
	var raw struct {
		Records []struct {
			TS     json.RawMessage `json:"ts"`
			Values []*float64      `json:"values"`
		} `json:"records"`
	}
	if err := json.NewDecoder(body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("JSON 无效: %w", err)
	}
	if raw.Records == nil {
		return nil, errors.New("缺少 records")
	}
	out := make([]store.Record, len(raw.Records))
	for i, rec := range raw.Records {
		if rec.Values == nil {
			return nil, fmt.Errorf("第 %d 条缺少 values", i+1)
		}
		ts, err := parseTS(rec.TS)
		if err != nil {
			return nil, fmt.Errorf("第 %d 条: %w", i+1, err)
		}
		out[i] = store.Record{UnixNano: ts, Values: floatValues(rec.Values)}
	}
	return out, nil
}

func readRange(r *http.Request) (int64, int64, error) {
	fromRaw := r.URL.Query().Get("from")
	toRaw := r.URL.Query().Get("to")
	if fromRaw == "" || toRaw == "" {
		return 0, 0, errors.New("from 和 to 都要提供")
	}
	from, err := parseTimestamp(fromRaw)
	if err != nil {
		return 0, 0, fmt.Errorf("from: %w", err)
	}
	to, err := parseTimestamp(toRaw)
	if err != nil {
		return 0, 0, fmt.Errorf("to: %w", err)
	}
	return from, to, nil
}

func parseTS(raw json.RawMessage) (int64, error) {
	raw = trimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return 0, errors.New("缺少时间戳")
	}
	if raw[0] == '"' {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return 0, err
		}
		return parseTimestamp(text)
	}
	n, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, errors.New("无法解析时间戳")
	}
	return normalizeUnix(n), nil
}

func parseTimestamp(text string) (int64, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0, errors.New("缺少时间戳")
	}
	if t, err := time.Parse(time.RFC3339Nano, text); err == nil {
		return t.UnixNano(), nil
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, errors.New("无法解析时间戳")
	}
	return normalizeUnix(n), nil
}

func normalizeUnix(n int64) int64 {
	abs := n
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs < 100_000_000_000:
		return n * int64(time.Second)
	case abs < 100_000_000_000_000:
		return n * int64(time.Millisecond)
	case abs < 100_000_000_000_000_000:
		return n * int64(time.Microsecond)
	default:
		return n
	}
}

func floatValues(ptrs []*float64) []float64 {
	out := make([]float64, len(ptrs))
	for i, p := range ptrs {
		if p == nil {
			out[i] = math.NaN()
		} else {
			out[i] = *p
		}
	}
	return out
}

func floatPtrs(vals []float64) []*float64 {
	out := make([]*float64, len(vals))
	for i, v := range vals {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		v := v
		out[i] = &v
	}
	return out
}

func toWire(recs []store.Record) []wireRecord {
	out := make([]wireRecord, len(recs))
	for i, rec := range recs {
		out[i] = wireRecord{
			TS:     time.Unix(0, rec.UnixNano).UTC().Format(time.RFC3339Nano),
			Values: floatPtrs(rec.Values),
		}
	}
	return out
}

func marshalSample(ts int64, values []float64) []byte {
	raw, err := json.Marshal(wireRecord{
		TS:     time.Unix(0, ts).UTC().Format(time.RFC3339Nano),
		Values: floatPtrs(values),
	})
	if err != nil {
		return []byte(`{"ts":"","values":null}`)
	}
	return raw
}

func writeSSE(w io.Writer, event string, payload []byte) error {
	if _, err := fmt.Fprintf(w, "event: %s\ndata: ", event); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	_, err := io.WriteString(w, "\n\n")
	return err
}

func trimSpace(b []byte) []byte {
	return []byte(strings.TrimSpace(string(b)))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeReadErr(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) || strings.Contains(err.Error(), "too large") {
		writeErr(w, http.StatusRequestEntityTooLarge, "请求体过大")
		return
	}
	writeErr(w, http.StatusBadRequest, err.Error())
}

func writeStoreErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrTimeRegression):
		writeErr(w, http.StatusConflict, err.Error())
	case errors.Is(err, store.ErrFieldCount):
		writeErr(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, store.ErrClosed):
		writeErr(w, http.StatusServiceUnavailable, err.Error())
	default:
		writeErr(w, http.StatusInternalServerError, err.Error())
	}
}
