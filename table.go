package deltalake

import (
	"io"
	"log/slog"
)

// todo: add lazy files - open only when accessed

const (
	_initActionCap = 50
)

type table struct {
	name    string
	columns []string

	files   []string // underlying table files
	actions []action

	dirty bool // if any data was changed in the transaction
}

func newTable(name string) *table {
	return &table{
		name: name,
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
}

func (tb *tableBuilder) add(a action) *tableBuilder {
	// todo: // refactor actions
	switch a.(type) {
	case *changeMetadata:
		cm := a.(*changeMetadata)
		tb.columns = cm.Columns
		return tb
	case *dataObjectAction:
		doa := a.(*dataObjectAction)
		tb.actions = append(tb.actions, a)
		tb.files = append(tb.files, doa.File)
		return tb
	default:
		slog.Error("unsuported action")
		return tb
	}
}

func (tb *tableBuilder) build() *table {
	return &table{
		name:    tb.name,
		columns: tb.columns,
		files:   tb.files,
		actions: tb.actions,
	}
}
