package uuidx

import (
	"strings"
	"testing"
)

func TestNewOperationID(t *testing.T) {
	tests := []struct {
		name string
		op   string
	}{
		{"empty op", ""},
		{"simple op", "login"},
		{"path op", "/group/get_joined_group_list"},
		{"path with trailing slash", "/auth/get_admin_token/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewOperationID(tt.op)
			if !strings.HasPrefix(got, "sdk_") {
				t.Errorf("NewOperationID() = %q, want prefix sdk_", got)
			}
			// 唯一性：两次生成不重复
			if got == NewOperationID(tt.op) {
				t.Error("NewOperationID() should be unique per call")
			}
			// 语义段核对
			switch tt.op {
			case "":
				if strings.Count(got, "_") != 1 {
					t.Errorf("empty op: got %q, want sdk_<uuid>", got)
				}
			case "login":
				if !strings.HasPrefix(got, "sdk_login_") {
					t.Errorf("got %q, want prefix sdk_login_", got)
				}
			default:
				if strings.Contains(got, "/") {
					t.Errorf("op slashes should be converted: %q", got)
				}
			}
		})
	}
}
