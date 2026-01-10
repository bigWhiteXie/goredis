package types

import (
	"testing"
)

func TestTypes_All(t *testing.T) {
	t.Run("CmdLine_IsWrite", func(t *testing.T) {
		tests := []struct {
			name string
			line CmdLine
			want bool
		}{
			{"empty", [][]byte{}, false},
			{"set_lower", [][]byte{[]byte("set")}, true},
			{"SET_upper", [][]byte{[]byte("SET")}, true},
			{"SeT_mixed", [][]byte{[]byte("SeT")}, true},
			{"get_read", [][]byte{[]byte("get")}, false},
			{"unknown", [][]byte{[]byte("foo")}, false},
			{"multi_args", [][]byte{[]byte("SET"), []byte("k"), []byte("v")}, true},
		}
		for _, tc := range tests {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				if got := tc.line.IsWrite(); got != tc.want {
					t.Errorf("IsWrite() = %v, want %v", got, tc.want)
				}
			})
		}
	})

	t.Run("DataEntity_Clone", func(t *testing.T) {
		t.Run("cloneable", func(t *testing.T) {
			// 自定义一个可克隆对象
			mock := &mockCloneable{val: 42}
			entity := &DataEntity{Data: mock}
			cloned := entity.Clone().(*DataEntity)
			if cloned.Data.(*mockCloneable).val != 42 {
				t.Errorf("cloned val = %v, want 42", cloned.Data.(*mockCloneable).val)
			}
			// 确保克隆的是新对象
			mock.val = 99
			if cloned.Data.(*mockCloneable).val != 42 {
				t.Error("clone should be deep")
			}
		})

		t.Run("not_cloneable", func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Error("expected panic for non-Cloneable Data")
				}
			}()
			entity := &DataEntity{Data: "not_cloneable"}
			_ = entity.Clone()
		})
	})
}

// ---------------- 辅助 ----------------

type mockCloneable struct {
	val int
}

func (m *mockCloneable) Clone() interface{} {
	return &mockCloneable{val: m.val}
}
