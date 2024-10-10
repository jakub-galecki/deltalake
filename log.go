package deltalake

import (
	"encoding/json"
	"io"
)

/*
todo:
	[] add log Checkpoints
*/

const logPrefix = "_log"

const (
	_add    = 0
	_remove = 1
)

type action interface {
	write(io.Writer) (int, error)
}

type changeMetadata struct {
	Table   string
	Columns []string
}

type dataObjectAction struct {
	Action int
	Table  string
	File   string
}

func createChangeMetadaAction(table string, cols []string) *changeMetadata {
	return &changeMetadata{
		Table:   table,
		Columns: cols,
	}
}

func (cm *changeMetadata) write(w io.Writer) (int, error) {
	raw, err := json.Marshal(cm)
	if err != nil {
		return 0, err
	}
	return w.Write(raw)
}

func createDataObjAction(table, file string, action int) *dataObjectAction {
	return &dataObjectAction{
		Action: action,
		Table:  table,
		File:   file,
	}
}

func (da *dataObjectAction) write(w io.Writer) (int, error) {
	raw, err := json.Marshal(da)
	if err != nil {
		return 0, err
	}
	return w.Write(raw)
}
