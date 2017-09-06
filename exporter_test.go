// exporter_test.go
package main

import (
	"fmt"
	"testing"
)

func TestNewFileName(t *testing.T) {
	fmt.Println("Hello World!")
	t.Error(newFileName())
}
