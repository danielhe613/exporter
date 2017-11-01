package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"testing"
	"time"
	//	"github.com/prometheus/common/log"
)

//decode json file.
func TestDecoding(t *testing.T) {
	ts := &TimeSerie{}

	buf := new(bytes.Buffer)
	buf.WriteString("")

	file, _ := os.Open("export/master-1-1-2017-09-14-15-3-51")
	defer func() {
		if file != nil {
			file.Close()
		}
	}()

	dec := json.NewDecoder(file)

	for i := 0; i < 2; i++ {
		dec.Decode(ts)
		fmt.Println(ts.Value)
	}
}
func TestCommitAndReset(t *testing.T) {

	CommitBatchSize = 1000
	MaxCommitInterval = time.Second * 5
	log.Println("Hello World!")

	fw := NewFileWriter("master", 1)
	defer fw.Close()

	ts := new(TimeSerie)
	ts.Metric = "sys_cpu_usage"
	ts.Timestamp = 1504779771421
	ts.Value = 25.000000

	ts.Tags = make(map[string]string)
	ts.Tags["host"] = "web001"
	ts.Tags["dc"] = "上海秋实"

	fw.AddTimeSerie(ts)

	ts.Reset()
	log.Println("Reset Timestamp to " + strconv.FormatInt(ts.Timestamp, 10))

	time.Sleep(time.Second * 10)

}

func TestCommitAndReset2(t *testing.T) {

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
