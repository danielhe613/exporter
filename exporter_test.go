package main

import (
	"fmt"
	"testing"
)

func TestNewFileName(t *testing.T) {
	fmt.Println("Hello World!")
	t.Error(newFileName())
}

func Test_compressFile(t *testing.T) {
	type args struct {
		filename string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.ls
		
		{"test1", args{"README.md"}, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compressFile(tt.args.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("compressFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("compressFile() = %v, want %v", got, tt.want)
			}
		})
	}
}
