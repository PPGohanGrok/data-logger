package httpapi

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"tsdb/internal/live"
	"tsdb/internal/store"
)

func newTestServer(t *testing.T, mutate func(*store.Options)) *httptest.Server {
	t.Helper()
	hub := live.New()
	opts := store.Options{
		DataDir:    t.TempDir(),
		Fields:     []string{"temp", "flow"},
		CacheHours: 3,
		Retention:  14 * 24 * time.Hour,
		OnSample: func(rec store.Record) {
			hub.Publish(live.Sample{Seq: rec.Seq, UnixNano: rec.UnixNano, Values: rec.Values})
		},
	}
	if mutate != nil {
		mutate(&opts)
	}
	st, err := store.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	api := &Server{
		Store:         st,
		Hub:           hub,
		Fields:        opts.Fields,
		RetentionDays: 14,
		CacheHours:    3,
	}
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(func() {
		srv.Close()
		_ = st.Close()
	})
	return srv
}

func postJSON(t *testing.T, url, body string) (int, []byte) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, raw
}

func readEvent(t *testing.T, r *bufio.Reader) (string, string) {
	t.Helper()
	var event, data string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if event != "" || data != "" {
				return event, data
			}
			continue
		}
		if strings.HasPrefix(line, "event:") {
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
	}
}

func TestWriteQueryLatestAndSchema(t *testing.T) {
	srv := newTestServer(t, nil)
	when := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	code, raw := postJSON(t, srv.URL+"/v1/write", `{"ts":"2026-10-08T12:00:00Z","values":[1.5,null]}`)
	if code != http.StatusOK {
		t.Fatalf("%d %s", code, raw)
	}
	code, raw = postJSON(t, srv.URL+"/v1/write", `{"ts":`+strconv.FormatInt(when.Add(time.Second).Unix(), 10)+`,"values":[2,3]}`)
	if code != http.StatusOK {
		t.Fatalf("unix 秒 %d %s", code, raw)
	}
	code, raw = postJSON(t, srv.URL+"/v1/write", `{"ts":"2026-10-08T11:59:59Z","values":[0,0]}`)
	if code != http.StatusConflict {
		t.Fatalf("倒退 %d %s", code, raw)
	}
	code, raw = postJSON(t, srv.URL+"/v1/write", `{"ts":"2026-10-08T12:00:02Z","values":[1]}`)
	if code != http.StatusBadRequest {
		t.Fatalf("字段数 %d %s", code, raw)
	}

	from := when.Format(time.RFC3339)
	to := when.Add(2 * time.Second).Format(time.RFC3339)
	resp, err := http.Get(srv.URL + "/v1/query?from=" + from + "&to=" + to)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Records []struct {
			TS     string     `json:"ts"`
			Values []*float64 `json:"values"`
		} `json:"records"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Records) != 2 || body.Records[0].Values[1] != nil || *body.Records[0].Values[0] != 1.5 {
		t.Fatalf("%+v", body.Records)
	}

	binResp, err := http.Get(srv.URL + "/v1/query?from=" + from + "&to=" + to + "&format=bin")
	if err != nil {
		t.Fatal(err)
	}
	defer binResp.Body.Close()
	if binResp.Header.Get("X-Count") != "2" || binResp.Header.Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("headers %+v", binResp.Header)
	}
	payload, err := io.ReadAll(binResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := store.Decode(payload[:len(payload)/2])
	if err != nil {
		t.Fatal(err)
	}
	if !math.IsNaN(rec.Values[1]) {
		t.Fatalf("二进制缺测 %+v", rec.Values)
	}

	latest, err := http.Get(srv.URL + "/v1/latest?n=1")
	if err != nil {
		t.Fatal(err)
	}
	defer latest.Body.Close()
	var latestBody struct {
		Records []struct {
			Values []*float64 `json:"values"`
		} `json:"records"`
	}
	if err := json.NewDecoder(latest.Body).Decode(&latestBody); err != nil {
		t.Fatal(err)
	}
	if len(latestBody.Records) != 1 || *latestBody.Records[0].Values[0] != 2 {
		t.Fatalf("%+v", latestBody.Records)
	}

	schemaResp, err := http.Get(srv.URL + "/v1/schema")
	if err != nil {
		t.Fatal(err)
	}
	defer schemaResp.Body.Close()
	if !strings.Contains(readAll(t, schemaResp.Body), "temp") {
		t.Fatal("schema 缺少字段")
	}
	health, err := http.Get(srv.URL + "/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	defer health.Body.Close()
	if got := health.Header.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("cors %q", got)
	}
	if !strings.Contains(readAll(t, health.Body), `"records":2`) {
		t.Fatal("health 条数不对")
	}

	page, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer page.Body.Close()
	html := readAll(t, page.Body)
	if !strings.Contains(html, "当前测点") || !strings.Contains(html, "EventSource") || !strings.Contains(html, "temp") {
		t.Fatal("页面缺少实时画面")
	}
}

func TestBinaryWriteAndBatch(t *testing.T) {
	srv := newTestServer(t, nil)
	when := time.Date(2026, 10, 8, 13, 0, 0, 0, time.UTC)
	raw := store.Encode(when.UnixNano(), []float64{9, 8})
	resp, err := http.Post(srv.URL+"/v1/write", "application/octet-stream", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("binary %d", resp.StatusCode)
	}
	batch := `{"records":[{"ts":"2026-10-08T13:00:01Z","values":[1,2]},{"ts":"2026-10-08T13:00:02Z","values":[3,4]}]}`
	code, body := postJSON(t, srv.URL+"/v1/write/batch", batch)
	if code != http.StatusOK {
		t.Fatalf("batch %d %s", code, body)
	}
	got, err := http.Get(srv.URL + "/v1/latest?n=3")
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	if !strings.Contains(readAll(t, got.Body), "2026-10-08T13:00:02Z") {
		t.Fatal("批量最后一条不在")
	}
}

func TestSampleArrivesBeforeSync(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	defer free()

	srv := newTestServer(t, func(o *store.Options) {
		o.BeforeSync = func() {
			close(entered)
			<-release
		}
	})
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/v1/live", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rd := bufio.NewReader(resp.Body)
	if ev, _, _ := readEventOK(rd); ev != "snapshot" {
		t.Fatalf("event %s", ev)
	}

	got := make(chan struct{})
	go func() {
		ev, _, err := readEventOK(rd)
		if err != nil || ev != "sample" {
			t.Errorf("sample %s %v", ev, err)
		}
		close(got)
	}()
	done := make(chan struct{})
	go func() {
		resp, err := http.Post(srv.URL+"/v1/write", "application/json", strings.NewReader(`{"ts":"2026-10-08T14:00:00Z","values":[1,2]}`))
		if err != nil {
			t.Errorf("write: %v", err)
		} else {
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("write %d %s", resp.StatusCode, raw)
			}
		}
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("没有进入刷盘")
	}
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("刷盘前没有收到样本")
	}
	select {
	case <-done:
		t.Fatal("写入在刷盘前返回了")
	default:
	}
	free()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("写入没有结束")
	}
}

func TestLiveLatency(t *testing.T) {
	srv := newTestServer(t, nil)
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/v1/live", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rd := bufio.NewReader(resp.Body)
	if ev, _ := readEvent(t, rd); ev != "snapshot" {
		t.Fatalf("event %s", ev)
	}
	code, raw := postJSON(t, srv.URL+"/v1/write", `{"ts":"2026-10-08T15:00:00Z","values":[1,1]}`)
	if code != http.StatusOK {
		t.Fatalf("warmup %d %s", code, raw)
	}
	if ev, _ := readEvent(t, rd); ev != "sample" {
		t.Fatalf("warmup event %s", ev)
	}

	start := time.Now()
	code, raw = postJSON(t, srv.URL+"/v1/write", `{"ts":"2026-10-08T15:00:01Z","values":[2,2]}`)
	writeDone := time.Now()
	if code != http.StatusOK {
		t.Fatalf("write %d %s", code, raw)
	}
	if ev, data := readEvent(t, rd); ev != "sample" || !strings.Contains(data, "15:00:01") {
		t.Fatalf("%s %s", ev, data)
	}
	recv := time.Now()
	if recv.After(writeDone) && recv.Sub(writeDone) > 20*time.Millisecond {
		t.Fatalf("写入返回后 %s 才收到，从发起到收到 %s", recv.Sub(writeDone), recv.Sub(start))
	}
}

func TestHeartbeat(t *testing.T) {
	srv := newTestServer(t, nil)
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/v1/live", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rd := bufio.NewReader(resp.Body)
	if ev, _ := readEvent(t, rd); ev != "snapshot" {
		t.Fatalf("event %s", ev)
	}
	ev, _ := readEvent(t, rd)
	if ev != "heartbeat" {
		t.Fatalf("event %s", ev)
	}
}

func readEventOK(r *bufio.Reader) (string, string, error) {
	var event, data string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return "", "", err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if event != "" || data != "" {
				return event, data, nil
			}
			continue
		}
		if strings.HasPrefix(line, "event:") {
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
	}
}

func readAll(t *testing.T, r io.Reader) string {
	t.Helper()
	raw, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
