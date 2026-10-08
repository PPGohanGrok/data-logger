//go:build windows

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
)

const serviceName = "Tsdb"

type svcHandler struct {
	run func(context.Context) error
}

func (h *svcHandler) Execute(args []string, reqs <-chan svc.ChangeRequest, changes chan<- svc.Status) (bool, uint32) {
	const accept = svc.AcceptStop | svc.AcceptShutdown
	changes <- svc.Status{State: svc.StartPending}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- h.run(ctx) }()
	changes <- svc.Status{State: svc.Running, Accepts: accept}
	for {
		select {
		case c := <-reqs:
			switch c.Cmd {
			case svc.Interrogate:
				changes <- svc.Status{State: svc.Running, Accepts: accept}
			case svc.Stop, svc.Shutdown:
				changes <- svc.Status{State: svc.StopPending}
				cancel()
				if err := <-done; err != nil {
					return false, 1
				}
				return false, 0
			}
		case err := <-done:
			if err != nil {
				return false, 1
			}
			return false, 0
		}
	}
}

func runningAsService() (bool, error) {
	return svc.IsWindowsService()
}

func runService(fn func(context.Context) error) error {
	return svc.Run(serviceName, &svcHandler{run: fn})
}

func runServiceCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("用法: tsdb service install|uninstall|start|stop")
	}
	switch args[0] {
	case "install":
		return installService(args[1:])
	case "uninstall":
		return uninstallService()
	case "start":
		return controlService(true)
	case "stop":
		return controlService(false)
	case "run":
		return runServe(args[1:], nil)
	default:
		return fmt.Errorf("未知服务命令 %s", args[0])
	}
}

func installService(args []string) error {
	fs := flag.NewFlagSet("install", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "tsdb.json", "配置文件路径")
	if err := fs.Parse(args); err != nil {
		return err
	}
	absConfig, err := filepath.Abs(*configPath)
	if err != nil {
		return err
	}
	if _, err := os.Stat(absConfig); err != nil {
		return fmt.Errorf("配置文件不存在: %s", absConfig)
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	m, err := mgr.Connect()
	if err != nil {
		return err
	}
	defer m.Disconnect()
	if existing, err := m.OpenService(serviceName); err == nil {
		existing.Close()
		return errors.New("服务已安装")
	}
	s, err := m.CreateService(serviceName, exe, mgr.Config{
		DisplayName: "时序存储",
		Description: "轻量时序数据库，对本机提供写入、按时间读取和实时推送",
		StartType:   mgr.StartAutomatic,
	}, "service", "run", "-config", absConfig)
	if err != nil {
		return err
	}
	defer s.Close()
	fmt.Printf("已安装服务 %s\n", serviceName)
	return nil
}

func uninstallService() error {
	m, err := mgr.Connect()
	if err != nil {
		return err
	}
	defer m.Disconnect()
	s, err := m.OpenService(serviceName)
	if err != nil {
		return err
	}
	defer s.Close()
	_, _ = s.Control(svc.Stop)
	if err := s.Delete(); err != nil {
		return err
	}
	fmt.Printf("已删除服务 %s\n", serviceName)
	return nil
}

func controlService(start bool) error {
	m, err := mgr.Connect()
	if err != nil {
		return err
	}
	defer m.Disconnect()
	s, err := m.OpenService(serviceName)
	if err != nil {
		return err
	}
	defer s.Close()
	if start {
		return s.Start()
	}
	_, err = s.Control(svc.Stop)
	return err
}
