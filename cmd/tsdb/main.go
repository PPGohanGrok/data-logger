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
	"tsdb/internal/tcpin"
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

画面默认在 http://127.0.0.1:8741/ 。
传感器中枢用 TCP 连接局域网端口，默认 0.0.0.0:8742。
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
	names := cfg.Names()
	st, err := store.Open(store.Options{
		DataDir:    cfg.DataDir,
		Fields:     names,
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
		Fields:        names,
		Codes:         cfg.Codes(),
		RetentionDays: cfg.RetentionDays,
		CacheHours:    cfg.CacheHours,
	}
	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errc := make(chan error, 2)
	go func() {
		log.Printf("画面 http://%s ，传感器 TCP %s ，数据目录 %s ，%d 个字段，保留 %d 天", cfg.Listen, cfg.TCPListen, cfg.DataDir, len(names), cfg.RetentionDays)
		errc <- srv.ListenAndServe()
	}()
	go func() {
		tcpSrv := &tcpin.Server{
			Addr:  cfg.TCPListen,
			Store: st,
			Codes: cfg.Codes(),
			Idle:  cfg.FrameIdle(),
		}
		if err := tcpSrv.Serve(ctx); err != nil {
			errc <- err
		}
	}()
	shutdown := func() {
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shut)
	}
	select {
	case <-ctx.Done():
		shutdown()
		return nil
	case err := <-errc:
		shutdown()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
