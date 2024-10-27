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

func newTableBuilder(name string) *tableBuilder {
	return &tableBuilder{
		name:    name,
		columns: make([]string, 0),
		files:   make([]string, 0),
		actions: make([]action, 0),
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
		name:    tb.name,
		columns: tb.columns,
		files:   tb.files,
		actions: tb.actions,
	}
}
