package main

import (
	"strconv"
	"time"
)

const MAX_AGGREGATION_NUMBER int = 4000

var FileSequenceNo int = 0

type TimeSeriesPackage struct {
	FileName        string
	TimeSeriesArray []*[]byte
	Count           int
}

func newFileName() string {
	now := time.Now()
	FileSequenceNo++
	return "ASSCY" + "-" + now.Format("2017-09-06") + "-" + strconv.Itoa(now.Hour()) + "-" + strconv.Itoa(now.Minute()) + "-" + strconv.Itoa(now.Second()) + "-" + strconv.Itoa(FileSequenceNo)
}

// NewTimeSeriesPackage is used as constructor for TimeSeriesPackage struct.
func NewTimeSeriesPackage() *TimeSeriesPackage {
	tsp := &TimeSeriesPackage{
		FileName:        newFileName(),
		TimeSeriesArray: make([]*[]byte, 0),
		Count:           0,
	}

	return tsp
}

//AddTimeSeries will add given compressed TimeSeries into this TimeSeriesPackage.
func (tsp *TimeSeriesPackage) AddTimeSeries(ts *[]byte) {

	tsp.TimeSeriesArray = append(tsp.TimeSeriesArray, ts)

	tsp.Count++
}

func (tsp *TimeSeriesPackage) isFull() bool {

	return tsp.Count >= MAX_AGGREGATION_NUMBER
}

func (tsp *TimeSeriesPackage) isNotEmpty() bool {

	return tsp.Count > 0
}
