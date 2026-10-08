package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"tsdb/internal/config"
	"tsdb/internal/httpapi"
	"tsdb/internal/live"
	"tsdb/internal/store"
)

func main() {
	log.SetFlags(log.LstdFlags)
	inSvc, err := runningAsService()
	if err != nil {
		log.Fatal(err)
	}
	if inSvc {
		if err := runService(func(ctx context.Context) error {
			return runServe(stripServiceArgs(os.Args[1:]), ctx)
		}); err != nil {
			log.Fatal(err)
		}
		return
	}

	args := os.Args[1:]
	cmd := "serve"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd = args[0]
		args = args[1:]
	}
	var runErr error
	switch cmd {
	case "serve":
		runErr = runServe(args, nil)
	case "service":
		runErr = runServiceCommand(args)
	case "help", "-h", "--help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if runErr != nil {
		log.Fatal(runErr)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `时序存储

用法:
  tsdb serve [-config tsdb.json]
  tsdb service install [-config tsdb.json]
  tsdb service uninstall
  tsdb service start
  tsdb service stop

默认监听 127.0.0.1:8741。当前值页面是 http://127.0.0.1:8741/
`)
}

func stripServiceArgs(args []string) []string {
	if len(args) > 0 && args[0] == "service" {
		args = args[1:]
	}
	if len(args) > 0 && args[0] == "run" {
		args = args[1:]
	}
	return args
}

func runServe(args []string, ctx context.Context) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "tsdb.json", "配置文件路径")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if ctx == nil {
		var stop context.CancelFunc
		ctx, stop = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
	}

	hub := live.New()
	st, err := store.Open(store.Options{
		DataDir:    cfg.DataDir,
		Fields:     cfg.Fields,
		Retention:  cfg.Retention(),
		CacheHours: cfg.CacheHours,
		OnSample: func(rec store.Record) {
			hub.Publish(live.Sample{
				Seq:      rec.Seq,
				UnixNano: rec.UnixNano,
				Values:   rec.Values,
			})
		},
	})
	if err != nil {
		return err
	}
	defer st.Close()
	go st.RetentionLoop(ctx)

	api := &httpapi.Server{
		Store:         st,
		Hub:           hub,
		Fields:        cfg.Fields,
		RetentionDays: cfg.RetentionDays,
		CacheHours:    cfg.CacheHours,
	}
	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		log.Printf("监听 http://%s ，数据目录 %s ，%d 个字段，保留 %d 天", cfg.Listen, cfg.DataDir, len(cfg.Fields), cfg.RetentionDays)
		errc <- srv.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shut)
		return nil
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
