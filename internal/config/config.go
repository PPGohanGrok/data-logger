// Package config 读取 tsdb.json。
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"tsdb/internal/schema"
)

const (
	DefaultListen     = "127.0.0.1:8741"
	DefaultRetention  = 14
	DefaultCacheHours = 3
)

// Config 是进程启动配置。DataDir 在 Load 之后是绝对路径。
type Config struct {
	Listen        string   `json:"listen"`
	DataDir       string   `json:"data_dir"`
	RetentionDays int      `json:"retention_days"`
	CacheHours    int      `json:"cache_hours"`
	Fields        []string `json:"fields"`
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
	if err := schema.Validate(cfg.Fields); err != nil {
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
