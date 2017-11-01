package main

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

//CommitBatchSize is the number of time series which will be saved into a file.
var CommitBatchSize int = 100000

var MaxCommitInterval = time.Second * 15

type TimeSerie struct {
	Metric    string            `json:"metric"`
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags"`
}

func NewTimeSerie() *TimeSerie {
	ts := new(TimeSerie)

	ts.Tags = make(map[string]string)
	return ts
}

//Reset assigns the initial value to TimeSerie.
func (ts *TimeSerie) Reset() {
	ts.Metric = ""
	ts.Timestamp = 0
	ts.Value = 0.0

	for key, _ := range ts.Tags {
		delete(ts.Tags, key)
	}
}

type FileWriter struct {
	app          string
	id           int
	fileSerialNo int         //Only increase after compressing the data file.
	fileName     string      //The name of current file being written.
	count        int         //The number of written time series batches, started from 0.
	file         *os.File    //The handler for current data file being written.
	timer        *time.Timer //timer used to send out MaxCommitInterval timeout signal.
	commitMux    sync.Mutex  //non-reentrant lock for File, used by functions AddTimeSeries(), CommitAndReset(), and Close() which will operate the file.
	stop         chan int    //channel used to send stop signal to goroutine handleTimeout().
	encoder      *json.Encoder
}

// NewFileWriter is used as constructor for FileWriter struct.
func NewFileWriter(app string, id int) *FileWriter {

	fw := &FileWriter{
		app:          app,
		id:           id,
		fileSerialNo: 1,
		fileName:     "",
		count:        0,
		file:         nil,
	}

	fw.stop = make(chan int, 2)

	fw.timer = time.NewTimer(MaxCommitInterval)
	go fw.handleTimeout()

	fw.reset()

	return fw
}

func (fw *FileWriter) handleTimeout() {
	log.Println("Enter handleTimeout()...")

	for {
		select {
		case <-fw.stop:
			return
		case <-fw.timer.C:
			log.Println("commit timeout!")
			fw.CommitAndReset()
		}
	}
}

//CommitAndReset tries to commit current round of data first, and then resets FileWriter itself.
//Called by Checkpoint Timer.
func (fw *FileWriter) CommitAndReset() error {

	fw.commitMux.Lock()
	defer fw.commitMux.Unlock()

	err := fw.commit()
	if err != nil {
		log.Printf("Error: %s occurred when committing the original data file %s! \n", err.Error(), fw.fileName)
	}

	return fw.reset()
}

//AddTimeSeries will add given compressed TimeSeries into this FileWriter.
//Called by exporter HTTP server.
func (fw *FileWriter) AddTimeSerie(v *TimeSerie) error {
	fw.commitMux.Lock()
	defer fw.commitMux.Unlock()

	if v == nil {
		log.Println("TimeSerie passed in is nil!")
	}

	err := fw.encoder.Encode(*v)
	//	_, err := fw.file.Write(*ts)
	if err != nil {
		log.Printf("Error: %s occurred when writing the original data file %s! \n", err.Error(), fw.fileName)
		return err
	}
	fw.count++

	if fw.count >= CommitBatchSize {
		err := fw.commit()
		if err != nil {
			log.Printf("Error: %s occurred when committing the original data file %s! \n", err.Error(), fw.fileName)
		}

		return fw.reset()
	}
	return nil
}

//Close tries to commit the bufferred first and then waits for being recycled.
func (fw *FileWriter) Close() error {
	fw.commitMux.Lock()
	defer fw.commitMux.Unlock()

	fw.stop <- 0

	return fw.commit()
}

//newFileName generates a new filename with the format as <context>-<id>-<fileSerialNo>-<yyyy-mm-dd-hh-mm-ss>
func (fw *FileWriter) newFileName() string {
	now := time.Now()

	return "export/" + fw.app + "-" + strconv.Itoa(fw.id) + "-" + strconv.Itoa(fw.fileSerialNo) + "-" + now.Format("2006-01-02") + "-" + strconv.Itoa(now.Hour()) + "-" + strconv.Itoa(now.Minute()) + "-" + strconv.Itoa(now.Second())
}

//reset tries to re-create filename, re-open the file ,reset the count to 0, and reset timer.
//That is to reset fileName, count and file pointer.
func (fw *FileWriter) reset() error {

	fw.fileName = fw.newFileName()
	fw.count = 0

	var err error
	fw.file, err = os.Create(fw.fileName)
	if err != nil {
		log.Println("Fatal: failed to create temporary file for metrics!!!")
		return err
	}

	fw.timer.Reset(MaxCommitInterval)

	fw.encoder = json.NewEncoder(fw.file)

	return nil
}

//commit tries to flush and close file, and then compresses it
func (fw *FileWriter) commit() error {

	fw.timer.Stop()

	defer func() {
		err := os.Remove(fw.fileName)
		if err != nil {
			log.Printf("Error: %s occurred when deleting the original data file %s. \n", err.Error(), fw.fileName)
		}
	}()

	err := fw.file.Close()
	if err != nil {
		log.Printf("Error:%s occurred when closing the original data file %s.", err.Error(), fw.fileName)
	}

	if fw.count <= 0 {
		log.Println("Nothing to commit!")
		return nil
	}

	_, err = fw.compressFile(fw.fileName)
	if err != nil {
		log.Println(err.Error())
	}

	return err
}

func (fw *FileWriter) compressFile(fileName string) (string, error) {

	tmpFileName := fileName + ".gz.compressing"
	dstFileName := fileName + ".gz"

	//Create tmp file for compressing.
	d, err := os.Create(tmpFileName)
	if err != nil {
		log.Println("Failed to create temp file for compressing due to error " + err.Error())
		return "", err
	}
	defer func() {
		d.Close()
		err := os.Rename(tmpFileName, dstFileName) //Rename .gz.compressing to .gz
		if err != nil {
			log.Println(err.Error())
			return
		}
		log.Println(dstFileName + " is saved!")
	}()

	//Creates gzip writer
	gw := gzip.NewWriter(d)
	defer gw.Close()

	//Opens source file
	srcFile, err := os.Open(fileName)
	if err != nil {
		log.Println(err.Error())
		return "", err
	}
	defer srcFile.Close()

	//Accumulates file serial no
	defer func() {
		fw.fileSerialNo++
	}()

	//Compresses by copying stream from source file to gzip writer
	_, err = io.Copy(gw, srcFile)
	gw.Flush()

	return dstFileName, err
}

func (fw *FileWriter) String() string {
	return "FileWriter" + strconv.Itoa(fw.id)
}
