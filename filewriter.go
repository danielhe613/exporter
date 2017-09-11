package main

import (
	"compress/gzip"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

const MAX_AGGREGATION_NUMBER int = 4000

var FileSequenceNo int = 0

type FileWriter struct {
	seqNo    int
	FileName string
	Count    int
}

func newFileName() string {
	now := time.Now()
	FileSequenceNo++
	return "ASSCY" + "-" + now.Format("2017-09-06") + "-" + strconv.Itoa(now.Hour()) + "-" + strconv.Itoa(now.Minute()) + "-" + strconv.Itoa(now.Second()) + "-" + strconv.Itoa(FileSequenceNo)
}

// NewFileWriter is used as constructor for FileWriter struct.
func NewFileWriter(seqNo int) *FileWriter {
	tsp := &FileWriter{
		seqNo:    seqNo,
		FileName: newFileName(),
		Count:    0,
	}

	return tsp
}

//AddTimeSeries will add given compressed TimeSeries into this FileWriter.
func (tsp *FileWriter) AddTimeSeries(ts *[]byte) {

	tsp.Count++
}

func (tsp *FileWriter) isFull() bool {

	return tsp.Count >= MAX_AGGREGATION_NUMBER
}

func (tsp *FileWriter) isNotEmpty() bool {

	return tsp.Count > 0
}

func (tsp *FileWriter) Commit() error {
	return nil
}

func (tsp *FileWriter) Close() error {
	return nil
}

func (tsp *FileWriter) String() string {
	return "FileWriter" + strconv.Itoa(tsp.seqNo)
}

func compressFile(filename string) (string, error) {

	dstFileName := filename + ".gz"
	d, _ := os.Create(dstFileName)
	defer d.Close()
	gw := gzip.NewWriter(d)
	defer gw.Close()

	tmp, err := os.Open(filename)
	defer func() {
		tmp.Close()
		err := os.Remove(filename)
		if err != nil {
			log.Printf("Error: %s occurred when deleting the original data file %s.", err.Error(), filename)
		}
	}()
	_, err = io.Copy(gw, tmp)
	gw.Flush()

	return dstFileName, err
}
