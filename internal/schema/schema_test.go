package schema

import "testing"

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
		many[i] = "f"
	}
	// 重复名会先失败；换成唯一名字再测上限。
	for i := range many {
		many[i] = string(rune('a'+i%26)) + string(rune('0'+i/26))
	}
	if err := Validate(many); err == nil {
		t.Fatal("超过 120 个字段应该被拒绝")
	}
	if err := Validate([]string{"反应器温度"}); err != nil {
		t.Fatal(err)
	}
}
