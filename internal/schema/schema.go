// Package schema 定义固定宽度记录的字段表。
// 字段顺序就是磁盘上 float64 的排列顺序，启动之后不能改。
package schema

import (
	"errors"
	"fmt"
	"hash/fnv"
	"unicode"
	"unicode/utf8"
)

// MaxFields 是一条样本里的浮点个数上限。
// 早期为了把单条记录压在 1024 字节内，上限是 120。
// 传感器一秒的文本可以超过 1024 字节（一般不超过 40960 位，即 5120 字节），
// 所以这里不再按 1024 字节倒推字段数。
const MaxFields = 1024

var (
	ErrNoFields  = errors.New("至少要有一个字段")
	ErrTooMany   = errors.New("字段太多")
	ErrBadName   = errors.New("字段名无效")
	ErrDuplicate = errors.New("字段名重复")
)

// Validate 检查字段名可以安全地写入配置、页面和文件路径以外的位置。
func Validate(fields []string) error {
	if len(fields) == 0 {
		return ErrNoFields
	}
	if len(fields) > MaxFields {
		return fmt.Errorf("%w：当前 %d 个，上限 %d", ErrTooMany, len(fields), MaxFields)
	}
	seen := make(map[string]struct{}, len(fields))
	for _, name := range fields {
		if err := checkName(name); err != nil {
			return err
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("%w: %s", ErrDuplicate, name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func checkName(name string) error {
	if name == "" || len(name) > 128 || !utf8.ValidString(name) {
		return fmt.Errorf("%w: %q", ErrBadName, name)
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f || r == '/' || r == '\\' || r == '<' || r == '>' || r == '&' {
			return fmt.Errorf("%w: %q", ErrBadName, name)
		}
		if unicode.IsControl(r) {
			return fmt.Errorf("%w: %q", ErrBadName, name)
		}
	}
	return nil
}

// Hash 是字段名序列的稳定指纹，写进每个小时文件的文件头。
func Hash(fields []string) uint64 {
	h := fnv.New64a()
	for _, name := range fields {
		_, _ = h.Write([]byte(name))
		_, _ = h.Write([]byte{0})
	}
	return h.Sum64()
}

// RecordSize 是一条记录的字节数：8 字节时间戳加上每个字段 8 字节。
func RecordSize(fieldCount int) int {
	return 8 + fieldCount*8
}
