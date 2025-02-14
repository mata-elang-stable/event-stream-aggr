package iplookup

import (
	"testing"
)

func TestClient_LookupIP(t *testing.T) {
	type fields struct {
		baseURL string
	}
	type args struct {
		ip string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Test LookupIP Should get success response",
			fields: fields{
				baseURL: "http://localhost:3000/",
			},
			args: args{
				ip: "1.1.1.1",
			},
			wantErr: false,
		},
		{
			name: "Test LookupIP Should get error response in private IP",
			fields: fields{
				baseURL: "http://localhost:3000/",
			},
			args: args{
				ip: "192.168.0.1",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewClient(tt.fields.baseURL)

			if err := c.LookupIP(tt.args.ip); (err != nil) != tt.wantErr {
				t.Errorf("LookupIP() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
