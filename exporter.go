// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/storage/remote"
)

const MAX_AGGREGATION_NUMBER int = 4000
const MAX_AGGREGATION_INTERVAL_MS time.Duration = 200 * time.Microsecond

var FileSequenceNo int = 1

type Exit struct{}

type Timeout struct {
	Now time.Time
}

type TimeSeriesPackage struct {
	FileName        string
	TimeSeriesArray []*[]byte
}

func newFileName() string {
	now := time.Now()
	return "ASSCY" + "-" + now.Format("2017-09-06") + "-" + strconv.Itoa(now.Hour()) + "-" + strconv.Itoa(now.Minute()) + "-" + strconv.Itoa(now.Second()) + "-" + strconv.Itoa(FileSequenceNo)
}

func Aggregate(from chan interface{}, to chan *TimeSeriesPackage, t1 *time.Timer) {

	//	var counter int = 0
	//	tsa := make([]*[]byte, MAX_AGGREGATION_NUMBER)

	var msg interface{}

	for {
		//		select {
		//		case ts := <-from:
		msg = <-from
		switch msg.(type) {
		case *[]byte:
			fmt.Println(*msg.(*[]byte))

			//Do aggregation...
			t1.Reset(time.Second * 10)

		case Timeout:

			fmt.Println(msg.(Timeout).Now.Format("2006-01-02 15:04:05.999999999 -0700 MST"))

			//Do aggregation...

			//Restart timer
			t1.Reset(time.Second * 10)
		case Exit:
			return
		}

		//		fmt.Println(*ts)
		//			tsa[counter] = ts
		//			counter++

		//			if counter >= MAX_AGGREGATION_NUMBER {
		//				//Stop timer

		//				tsp := new(TimeSeriesPackage)
		//				tsp.FileName = newFileName()
		//				tsp.TimeSeriesArray = tsa

		//				to <- tsp

		//				//Restart timer
		//			}

		//	case <-abort:
		//		fmt.Printf("Aborted!\n")
		//		default:
		//			time.Sleep(MAX_AGGREGATION_INTERVAL_MS)
		//		}

	}
}

func Export(from chan *TimeSeriesPackage) {
	for {
		tsp := <-from
		exportToFile(tsp)
		//	case <-abort:
		//		fmt.Printf("Aborted!\n")
	}
}

func exportToFile(tsp *TimeSeriesPackage) {

	defer func() {
		if p := recover(); p != nil {
			log.Panicln("Export to file error: %v", p)
		}
	}()

	compressed := tsp.TimeSeriesArray[0]

	reqBuf, err := snappy.Decode(nil, *compressed)
	if err != nil {
		log.Panicln(err.Error())
		return
	}

	var req remote.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		log.Panicln(err.Error())
		return
	}

	//fmt.Printf("Total = %d, ContentLength = %d\n", len(req.Timeseries), r.ContentLength)

	for _, ts := range req.Timeseries {
		m := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			m[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}
		fmt.Println(m)

		for _, s := range ts.Samples {
			fmt.Printf("  %f %d\n", s.Value, s.TimestampMs)
		}
	}
}

func handleTimeout(t *time.Timer, to chan interface{}) {
	for {
		to <- Timeout{<-t.C}
	}
}

func main() {

	//Channel for compressed time series from HTTP handler
	tsch := make(chan interface{}, 10000)

	//Channel for file writers
	tspch := make(chan *TimeSeriesPackage, 100)

	t1 := time.NewTimer(time.Second * 2)
	go handleTimeout(t1, tsch)

	//Start aggregator and file writer goroutines.
	go Aggregate(tsch, tspch, t1)
	go Export(tspch)

	//Launch HTTP server
	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		tsch <- &compressed
	})

	http.HandleFunc("/shutdown", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			tsch <- Exit{}
		}
	})

	log.Fatal(http.ListenAndServe(":1234", nil))
}
