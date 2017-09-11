package main

import "testing"
import "fmt"

func TestDefaultPooledObjectFactory(t *testing.T) {
	factory := &FileWriterFactory{}
	obj, _ := factory.MakeObject()

	fmt.Println(obj.Object)
}
