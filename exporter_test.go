package main

import (
	"testing"
	"time"

	pool "github.com/jolestar/go-commons-pool"
)

func TestPool(t *testing.T) {
	//Initializes the FileWriter pool
	fwf, _ := NewFileWriterFactory("master")
	p := pool.NewObjectPoolWithDefaultConfig(fwf)
	p.Config.MaxTotal = 100
	obj1, _ := p.BorrowObject()
	defer p.ReturnObject(obj1)

	obj2, _ := p.BorrowObject()
	defer p.ReturnObject(obj2)

}

func TestChannelClosed(t *testing.T) {

	ch := make(chan int, 10)

	close(ch)

	go func() {

		time.Sleep(time.Second * 2)
		ch <- 1

	}()

}
