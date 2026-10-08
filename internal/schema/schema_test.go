package schema

import (
	"fmt"
	"testing"
)

func TestHashStable(t *testing.T) {
	a := Hash([]string{"temp", "flow"})
	b := Hash([]string{"temp", "flow"})
	if a == 0 || a != b {
		t.Fatalf("hash = %d", a)
	}
	if Hash([]string{"flow", "temp"}) == a {
		t.Fatal("字段顺序应该影响哈希")
	}
}

func TestValidate(t *testing.T) {
	if err := Validate(nil); err == nil {
		t.Fatal("空字段应该被拒绝")
	}
	if err := Validate([]string{"a", "a"}); err == nil {
		t.Fatal("重复字段应该被拒绝")
	}
	if err := Validate([]string{"a/b"}); err == nil {
		t.Fatal("斜杠应该被拒绝")
	}
	many := make([]string, MaxFields+1)
	for i := range many {
		many[i] = fmt.Sprintf("f%d", i)
	}
	if err := Validate(many); err == nil {
		t.Fatal("超过字段上限应该被拒绝")
	}
	if err := Validate(makeNames(MaxFields)); err != nil {
		t.Fatal(err)
	}
	if err := Validate([]string{"反应器温度"}); err != nil {
		t.Fatal(err)
	}
}

func makeNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("f%d", i)
	}
	return names
}
