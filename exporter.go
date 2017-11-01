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
	"math"
	"net/http"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	pool "github.com/jolestar/go-commons-pool"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
)

func IsNotExist(path string) bool {
	_, err := os.Stat(path)
	return os.IsNotExist(err)
}

func main() {

	//Check if export folder exists or not
	if IsNotExist("export") {
		log.Fatalln("./export folder for exported data files does NOT exist! Please create and make it writable.")
	}

	//Initializes the FileWriter pool
	fwf, _ := NewFileWriterFactory("master")
	fwpool := pool.NewObjectPoolWithDefaultConfig(fwf)
	fwpool.Config.MaxTotal = 2

	//Launch HTTP server
	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		pooledObject, _ := fwpool.BorrowObject()
		defer fwpool.ReturnObject(pooledObject)
		fw := pooledObject.(*FileWriter)

		fmt.Printf("Got FileWriter %d   %d, %d, %d \n", fw.id, fwpool.GetNumActive(), fwpool.GetNumIdle(), fwpool.GetDestroyedCount())

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

		timeSerie := NewTimeSerie()

		for _, ts := range req.Timeseries {

			//Look for MetricNameLabel
			for _, l := range ts.Labels {
				if l.Name == model.MetricNameLabel {
					timeSerie.Metric = l.Value
				}
			}
			if len(timeSerie.Metric) == 0 { //empty
				continue
			}
			for _, s := range ts.Samples {

				if math.IsNaN(s.Value) {
					continue //skip the NaN value
				}

				timeSerie.Timestamp = s.TimestampMs
				timeSerie.Value = s.Value

				for _, l := range ts.Labels {
					if l.Name == model.MetricNameLabel {
						continue //skip label __name__
					}
					timeSerie.Tags[l.Name] = l.Value
				}

				fw.AddTimeSerie(timeSerie)
				timeSerie.Reset()
			}

		}

	})

	log.Fatal(http.ListenAndServe(":1234", nil))
}
