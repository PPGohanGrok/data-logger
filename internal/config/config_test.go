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
	if cfg.TCPListen != DefaultTCPListen {
		t.Fatalf("tcp %s", cfg.TCPListen)
	}
}

func TestLoadCodedFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tsdb.json")
	raw := []byte(`{"tcp_listen":"192.168.1.8:8742","fields":[{"code":"0108","name":"井深"},{"code":"0110","name":"钻头深度"}]}`)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TCPListen != "192.168.1.8:8742" {
		t.Fatalf("tcp %s", cfg.TCPListen)
	}
	if len(cfg.Names()) != 2 || cfg.Names()[0] != "井深" || cfg.Codes()[1] != "0110" {
		t.Fatalf("%+v", cfg.Fields)
	}
}

func TestLoadRejectsBadCode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tsdb.json")
	raw := []byte(`{"fields":[{"code":"108","name":"井深"}]}`)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("三位识别码应该失败")
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
