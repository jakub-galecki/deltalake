package deltalake

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
)

// todo: add lazy files - open only when accessed

const (
	_initActionCap = 50
)

var (
	ErrIteratorExhausted = errors.New("iterator exhausted")
)

type table struct {
	name    string
	columns []string

	files   []string // underlying table files
	actions []action

	dirty           bool // if any data was changed in the Transaction
	externalStorage ObjectStorage
}

func newTable(name string, storage ObjectStorage) *table {
	return &table{
		name:            name,
		externalStorage: storage,
	}
}

func (t *table) write(buf io.Writer) error {
	return nil
}

// tableBuilder builds a new table using existing logs
// stored in underlying files
type tableBuilder struct {
	name    string
	columns []string
	files   []string
	actions []action

	storage ObjectStorage
}

func newTableBuilder(name string, storage ObjectStorage) *tableBuilder {
	return &tableBuilder{
		name:    name,
		columns: make([]string, 0),
		files:   make([]string, 0),
		actions: make([]action, 0),
		storage: storage,
	}
}

func (tb *tableBuilder) add(a action) *tableBuilder {
	// todo: // refactor actions
	switch a := a.(type) {
	case *changeMetadata:
		tb.columns = a.Columns
		return tb
	case *dataObjectAction:
		tb.actions = append(tb.actions, a)
		tb.files = append(tb.files, a.File)
		return tb
	default:
		slog.Error("unsuported action")
		return tb
	}
}

func (tb *tableBuilder) build() *table {
	return &table{
		name:            tb.name,
		columns:         tb.columns,
		files:           tb.files,
		actions:         tb.actions,
		externalStorage: tb.storage,
	}
}

func (t *table) scan() *tableIt {
	tt := &tableIt{
		table:        t,
		tablePointer: 0,
		filePointer:  0,
	}
	return tt
}

// todo: add logging

type tableIt struct {
	table        *table
	tablePointer int
	filePointer  int

	buf [][]any
}

func (tt *tableIt) First() ([]any, error) {
	tt.filePointer = 0
	tt.tablePointer = 0

	err := tt.moveFile()
	if err != nil {
		return nil, err
	}
	return tt.Next()
}

func (tt *tableIt) Next() ([]any, error) {
	if tt.tablePointer == len(tt.buf) {
		err := tt.moveFile()
		if err != nil {
			return nil, err
		}
		tt.tablePointer = 0
	}
	if len(tt.buf) < tt.tablePointer {
		return nil, errors.New("empty table buffer")
	}
	defer func() {
		tt.tablePointer++
	}()
	return tt.buf[tt.tablePointer], nil
}

func (tt *tableIt) moveFile() error {
	if len(tt.table.files) <= tt.filePointer {
		return ErrIteratorExhausted
	}
	err := tt.loadFile(tt.table.files[tt.filePointer])
	if err != nil {
		return err
	}
	tt.filePointer++
	return nil
}

func (tt *tableIt) loadFile(file string) error {
	rd, err := tt.table.externalStorage.Read(file)
	if err != nil {
		return err
	}
	raw, err := io.ReadAll(rd)
	if err != nil {
		return err
	}
	var do dataObject
	err = json.Unmarshal(raw, &do)
	if err != nil {
		return err
	}
	if do.Table != tt.table.name {
		return errors.New("wrong data object read")
	}
	tt.buf = do.Data
	return nil
}
