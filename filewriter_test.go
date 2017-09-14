package main

import (
	"log"

	"testing"
	"time"
	//	"github.com/prometheus/common/log"
)

func TestCommitAndReset(t *testing.T) {

	CommitBatchSize = 1000
	MaxCommitInterval = time.Second * 5
	log.Println("Hello World!")

	fw := NewFileWriter("master", 1)
	defer fw.Close()

	ts := []byte("Hi,Apple!\n")

	for i := 0; i < 500; i++ {
		fw.AddTimeSeries(&ts)
	}

	ts = []byte("Hi,Peach!\n")

	log.Println("Go to sleep!")
	time.Sleep(time.Second * 8)

	for i := 0; i < 2000; i++ {
		fw.AddTimeSeries(&ts)
	}
	log.Println("Go to sleep!")
	time.Sleep(time.Second * 11)

	ts = []byte("Hi,Orange!\n")
	for i := 0; i < 2000; i++ {
		fw.AddTimeSeries(&ts)
	}

}

// func Test_compressFile(t *testing.T) {
// 	type args struct {
// 		filename string
// 	}
// 	tests := []struct {
// 		name    string
// 		args    args
// 		want    string
// 		wantErr bool
// 	}{
// 		// TODO: Add test cases.ls

// 		{"test1", args{"README.md"}, "", false},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			got, err := compressFile(tt.args.filename)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("compressFile() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			if got != tt.want {
// 				t.Errorf("compressFile() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }
