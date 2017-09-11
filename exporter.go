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

	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
)

const T1_TIMEOUT = time.Second * 5

//Events:
// type Exit struct{}
// type Timeout struct {
// 	Now time.Time
// }

//Runnable
// func aggregate(from chan *[]byte, to chan *TimeSeriesPackage, t1 *time.Timer) {

// 	var tsp *TimeSeriesPackage
// 	tsp = NewTimeSeriesPackage()

// 	for {
// 		select {
// 		case ts := <-from:

// 			tsp.AddTimeSeries(ts)
// 			if tsp.isFull() {
// 				tsp = pack(tsp, to, t1)
// 			}

// 		case <-t1.C:
// 			// fmt.Printf("Timeout!\n")

// 			if tsp.isNotEmpty() {
// 				//if counter > 0 send to export().
// 				tsp = pack(tsp, to, t1)
// 			} else {
// 				// fmt.Println("Reset timer...")
// 				t1.Reset(T1_TIMEOUT)
// 			}
// 		}

// 	}
// }

// func pack(tsp *TimeSeriesPackage, to chan *TimeSeriesPackage, t1 *time.Timer) *TimeSeriesPackage {
// 	fmt.Println("Pack...")
// 	//Stop timer
// 	t1.Stop()

// 	//Send to export()
// 	to <- tsp

// 	// New TimeSeriesPackage
// 	newtsp := NewTimeSeriesPackage()
// 	//Restart timer
// 	t1.Reset(T1_TIMEOUT)

// 	return newtsp
// }

// //Runnable export(),
// func export(from chan *TimeSeriesPackage) {
// 	for {
// 		tsp := <-from
// 		processTimeSeriesPackage(tsp)
// 	}
// }

// func processTimeSeriesPackage(tsp *TimeSeriesPackage) {
// 	defer func() {
// 		if p := recover(); p != nil {
// 			log.Println(p)
// 		}
// 	}()

// 	writeToFile(tsp)
// 	compressFile(tsp.FileName)
// }

// func writeToFile(tsp *TimeSeriesPackage) {

// 	file, _ := os.Create(tsp.FileName)
// 	defer func() {
// 		if file != nil {
// 			file.Close()
// 		}
// 	}()

// 	for _, compressed := range tsp.TimeSeriesArray {
// 		if compressed == nil {
// 			log.Println("Gotten compressed request body from prometheus is nil!")
// 			continue
// 		}

// 		// Write compressed
// 		reqBuf, err := snappy.Decode(nil, *compressed)
// 		if err != nil {
// 			log.Println(err.Error())
// 			continue
// 		}
// 		var req remote.WriteRequest
// 		if err := proto.Unmarshal(reqBuf, &req); err != nil {
// 			log.Println(err.Error())
// 			continue
// 		}

// 		var (
// 			metricName *string
// 		)
// 		for _, ts := range req.Timeseries {

// 			//Look for MetricNameLabel
// 			for _, l := range ts.Labels {
// 				if l.Name == model.MetricNameLabel {
// 					metricName = &(l.Value)
// 				}
// 			}
// 			if metricName == nil {
// 				continue
// 			}
// 			for _, s := range ts.Samples {

// 				if math.IsNaN(s.Value) {
// 					continue //skip the NaN value
// 				}
// 				_, err = file.WriteString(*metricName + " " + strconv.FormatInt(s.TimestampMs, 10) + " " + strconv.FormatFloat(s.Value, 'f', 6, 64) + " ")

// 				for _, l := range ts.Labels {
// 					if l.Name == model.MetricNameLabel {
// 						continue //skip label __name__
// 					}
// 					_, err = file.WriteString(l.Name + "=" + l.Value + " ")
// 				}
// 				_, err = file.WriteString("\n")
// 			}

// 		}

// 		// _, err = file.Write(buf)
// 		if err != nil {
// 			log.Printf("Error: %s raised when writing metric %s to file %s \n", err.Error(), *metricName, tsp.FileName)
// 		}

// 	}

// }

func main() {

	// var (
	// 	//Channel for compressed time series from HTTP handler, make sure not any delay impacts on the HTTP handler side.
	// 	tsch = make(chan *[]byte, 10000)

	// 	//Channel for file writers
	// 	tspch = make(chan *TimeSeriesPackage, 100)

	// 	t1 = time.NewTimer(T1_TIMEOUT)
	// )

	//Initializes the FileWriter pool
	// p := pool.NewObjectPoolWithDefaultConfig(pool.NewPooledObjectFactorySimple(
	// 	func() (interface{}, error) {
	// 		return &MyPoolObject{}, nil
	// 	}))

	//Launch HTTP server
	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req remote.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		fmt.Printf("Total = %d, ContentLength = %d\n", len(req.Timeseries), r.ContentLength)

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
	})

	log.Fatal(http.ListenAndServe(":1234", nil))
}
