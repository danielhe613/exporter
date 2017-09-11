package main

import (
	//	"log"

	"github.com/jolestar/go-commons-pool"
	"github.com/prometheus/common/log"
)

//FileWriterFactory is the factory for FileWriters.
type FileWriterFactory struct {
	lastSeqNo int
}

func NewFileWriterFactory() (*FileWriterFactory, error) {
	return &FileWriterFactory{lastSeqNo: 0}, nil
}

//MakeObject returns a newly created pooled object.
func (f *FileWriterFactory) MakeObject() (*pool.PooledObject, error) {

	f.lastSeqNo += 1
	fileWriter := NewFileWriter(f.lastSeqNo)
	return pool.NewPooledObject(fileWriter), nil
}

func (f *FileWriterFactory) DestroyObject(object *pool.PooledObject) error {

	fw := object.Object.(FileWriter)

	err := fw.Close()

	if err != nil {
		log.Errorln(err)
	}
	return nil
}

func (f *FileWriterFactory) ValidateObject(object *pool.PooledObject) bool {
	//do validate
	return true
}

func (f *FileWriterFactory) ActivateObject(object *pool.PooledObject) error {
	//do activate
	return nil
}

func (f *FileWriterFactory) PassivateObject(object *pool.PooledObject) error {
	fw := object.Object.(*FileWriter)
	err := fw.Commit()
	if err != nil {
		log.Errorln(err)
	}
	return err
}
