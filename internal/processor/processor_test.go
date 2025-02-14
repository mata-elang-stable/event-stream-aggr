package processor

import "testing"

func Test_parseUnixMicroTimestampToString(t *testing.T) {
	type args struct {
		t int64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test 1: Should return the correct date in string format",
			args: args{t: 1738296906927463},
			want: "2025-01-31T04:15:06.927Z",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseUnixMicroTimestampToString(tt.args.t); got != tt.want {
				t.Errorf("parseUnixMicroTimestampToString() = %v, want %v", got, tt.want)
				return
			}
			t.Logf("Got the correct date in string format: %v", tt.want)
		})
	}
}
