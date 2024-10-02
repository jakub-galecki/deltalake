package main

/*
todo:
	[] add log Checkpoints
*/

const logSubDir = "_log"

type changeMetadata struct {
	table string
	cols  []string
}

const (
	_add    = 0
	_remove = 1
)

type dataObjectAction struct {
	action int
	table  string
	file   string
}

type jsonable interface {
	Json() []byte
}

func createChangeMetadaAction(table string, cols []string) *changeMetadata {
	return &changeMetadata{
		table: table,
		cols:  cols,
	}
}

func createDataObjAction(table, file string, action int) *dataObjectAction {
	return &dataObjectAction{
		action: action,
		table:  table,
		file:   file,
	}
}
