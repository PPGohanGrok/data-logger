//go:build !windows

package main

import (
	"context"
	"errors"
)

func runningAsService() (bool, error) { return false, nil }

func runService(func(context.Context) error) error {
	return errors.New("Windows 服务仅能在 Windows 上运行")
}

func runServiceCommand([]string) error {
	return errors.New("Windows 服务命令仅能在 Windows 上使用")
}
