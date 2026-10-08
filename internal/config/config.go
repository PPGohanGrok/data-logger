// Package config 读取 tsdb.json。
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"unicode"

	"tsdb/internal/schema"
)

const (
	DefaultListen     = "127.0.0.1:8741"
	DefaultTCPListen  = "0.0.0.0:8742"
	DefaultRetention  = 14
	DefaultCacheHours = 3
	DefaultFrameIdle  = 200
)

// Field 是一个测点。Code 是传感器报文里的四位识别码，Name 是画面上的名字。
type Field struct {
	Code string
	Name string
}

// Config 是进程启动配置。DataDir 在 Load 之后是绝对路径。
type Config struct {
	Listen        string
	TCPListen     string
	DataDir       string
	RetentionDays int
	CacheHours    int
	FrameIdleMS   int
	Fields        []Field
}

// UnmarshalJSON 同时接受 "fields": ["名字"] 和 {"code":"0108","name":"井深"}。
func (c *Config) UnmarshalJSON(data []byte) error {
	var raw struct {
		Listen        string            `json:"listen"`
		TCPListen     string            `json:"tcp_listen"`
		DataDir       string            `json:"data_dir"`
		RetentionDays int               `json:"retention_days"`
		CacheHours    int               `json:"cache_hours"`
		FrameIdleMS   int               `json:"frame_idle_ms"`
		Fields        []json.RawMessage `json:"fields"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	fields := make([]Field, 0, len(raw.Fields))
	for _, item := range raw.Fields {
		item = bytes.TrimSpace(item)
		if len(item) == 0 {
			continue
		}
		if item[0] == '"' {
			var name string
			if err := json.Unmarshal(item, &name); err != nil {
				return err
			}
			fields = append(fields, Field{Name: name})
			continue
		}
		var obj struct {
			Code string `json:"code"`
			Name string `json:"name"`
		}
		if err := json.Unmarshal(item, &obj); err != nil {
			return fmt.Errorf("字段配置无效: %w", err)
		}
		fields = append(fields, Field{Code: obj.Code, Name: obj.Name})
	}
	*c = Config{
		Listen:        raw.Listen,
		TCPListen:     raw.TCPListen,
		DataDir:       raw.DataDir,
		RetentionDays: raw.RetentionDays,
		CacheHours:    raw.CacheHours,
		FrameIdleMS:   raw.FrameIdleMS,
		Fields:        fields,
	}
	return nil
}

// Load 读取配置。相对数据目录相对于配置文件所在目录，而不是进程的工作目录。
// Windows 服务的工作目录通常是 System32，不能拿它当基准。
func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("读取配置: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return Config{}, fmt.Errorf("解析配置: %w", err)
	}
	if cfg.Listen == "" {
		cfg.Listen = DefaultListen
	}
	if cfg.RetentionDays == 0 {
		cfg.RetentionDays = DefaultRetention
	}
	if cfg.RetentionDays < 1 {
		return Config{}, fmt.Errorf("retention_days 必须大于 0")
	}
	if cfg.CacheHours == 0 {
		cfg.CacheHours = DefaultCacheHours
	}
	if cfg.CacheHours < 1 {
		return Config{}, fmt.Errorf("cache_hours 必须大于 0")
	}
	if cfg.TCPListen == "" {
		cfg.TCPListen = DefaultTCPListen
	}
	if cfg.FrameIdleMS == 0 {
		cfg.FrameIdleMS = DefaultFrameIdle
	}
	if cfg.FrameIdleMS < 1 {
		return Config{}, fmt.Errorf("frame_idle_ms 必须大于 0")
	}
	fields, err := normalizeFields(cfg.Fields)
	if err != nil {
		return Config{}, err
	}
	cfg.Fields = fields
	if err := schema.Validate(cfg.Names()); err != nil {
		return Config{}, err
	}
	absConfig, err := filepath.Abs(path)
	if err != nil {
		return Config{}, err
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "data"
	}
	if !filepath.IsAbs(cfg.DataDir) {
		cfg.DataDir = filepath.Join(filepath.Dir(absConfig), cfg.DataDir)
	}
	return cfg, nil
}

// Retention 返回保留时长。
func (c Config) Retention() time.Duration {
	return time.Duration(c.RetentionDays) * 24 * time.Hour
}

// FrameIdle 是一帧报文停顿多久后先写入，不必干等下一帧。
func (c Config) FrameIdle() time.Duration {
	return time.Duration(c.FrameIdleMS) * time.Millisecond
}

// Names 是磁盘和画面上的字段顺序。
func (c Config) Names() []string {
	names := make([]string, len(c.Fields))
	for i, f := range c.Fields {
		names[i] = f.Name
	}
	return names
}

// Codes 与 Names 对齐。没有识别码的字段是空字符串。
func (c Config) Codes() []string {
	codes := make([]string, len(c.Fields))
	for i, f := range c.Fields {
		codes[i] = f.Code
	}
	return codes
}

func normalizeFields(fields []Field) ([]Field, error) {
	seen := make(map[string]struct{}, len(fields))
	out := make([]Field, len(fields))
	for i, f := range fields {
		if f.Name == "" {
			f.Name = f.Code
		}
		if f.Code != "" {
			if len(f.Code) != 4 || !digits(f.Code) {
				return nil, fmt.Errorf("识别码必须是四位数字: %s", f.Code)
			}
			if _, ok := seen[f.Code]; ok {
				return nil, fmt.Errorf("识别码重复: %s", f.Code)
			}
			seen[f.Code] = struct{}{}
		}
		out[i] = f
	}
	return out, nil
}

func digits(s string) bool {
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return s != ""
}
