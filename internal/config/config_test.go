package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRelativeDataDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tsdb.json")
	raw := []byte(`{"fields":["temp","flow"]}`)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != DefaultListen || cfg.RetentionDays != DefaultRetention || cfg.CacheHours != DefaultCacheHours {
		t.Fatalf("%+v", cfg)
	}
	if cfg.DataDir != filepath.Join(dir, "data") {
		t.Fatalf("data dir %s", cfg.DataDir)
	}
}

func TestLoadRejectsBadRetention(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tsdb.json")
	raw := []byte(`{"retention_days":-1,"fields":["temp"]}`)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("负数保留天数应该失败")
	}
}
