package deltalake

import (
	"encoding/json"
	"fmt"
)

const (
	_dataObjectSize = 1024
)

type dataObject struct {
	Id    string // generated uuid
	Table string
	Data  [][]any
	Size  int

	fileName string
}

func (do *dataObject) persist(objStorage ObjectStorage) error {
	raw, err := json.Marshal(do)
	if err != nil {
		return err
	}

	err = objStorage.Write(do.generateFileName(), raw)
	if err != nil {
		return err
	}
	return nil
}

func (do *dataObject) generateFileName() string {
	if do.fileName != "" {
		return do.fileName
	}
	do.fileName = fmt.Sprintf("_table_%s_%s", do.Table, do.Id)
	return do.fileName
}
