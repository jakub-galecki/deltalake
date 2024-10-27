package deltalake

import (
	"encoding/json"
	"fmt"
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
	serialize() ([]byte, error)
	getKind() LogKind
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

func newChangeMetadaAction(table string, cols []string) *changeMetadata {
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

func (cm *changeMetadata) serialize() ([]byte, error) {
	return json.Marshal(cm)
}

func (cm *changeMetadata) getKind() LogKind {
	return ChangeMetadata
}

func newDataObjAction(table, file string, action int) *dataObjectAction {
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

func (da *dataObjectAction) serialize() ([]byte, error) {
	return json.Marshal(da)
}

func (da *dataObjectAction) getKind() LogKind {
	return DataObject
}

type LogKind int

const (
	ChangeMetadata LogKind = iota
	DataObject
)

type logEntry struct {
	Kind LogKind
	Raw  []byte
}

func newLogEntry(a action) (*logEntry, error) {
	raw, err := a.serialize()
	if err != nil {
		return nil, err
	}
	return &logEntry{
		Kind: a.getKind(),
		Raw:  raw,
	}, nil
}

type logs []*logEntry

func newLogs() logs {
	return make(logs, 0)
}

func (l logs) append(entry *logEntry) logs {
	return append(l, entry)
}

func (l logs) serialize() ([]byte, error) {
	return json.Marshal(l)
}

func (l logs) deserialize(data []byte) (logs, error) {
	err := json.Unmarshal(data, &l)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (le *logEntry) action() (action, error) {
	switch le.Kind {
	case ChangeMetadata:
		cm := changeMetadata{}
		err := json.Unmarshal(le.Raw, &cm)
		if err != nil {
			return nil, err
		}
		return &cm, nil
	case DataObject:
		do := dataObjectAction{}
		err := json.Unmarshal(le.Raw, &do)
		if err != nil {
			return nil, err
		}
		return &do, nil
	default:
		return nil, fmt.Errorf("unknown log kind %d", le.Kind)
	}
}

func (l logs) actions() ([]action, error) {
	res := make([]action, 0, len(l))
	for _, entry := range l {
		a, err := entry.action()
		if err != nil {
			return nil, err
		}
		res = append(res, a)
	}
	return res, nil
}
