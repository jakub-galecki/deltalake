package deltalake

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"go.uber.org/multierr"
)

var transactionPool = sync.Pool{
	New: func() any {
		return new(transaction)
	},
}

type transaction struct {
	id int64

	d *delta

	// snapshot []any
	// load tables lazily
	tables map[string]*table // open tables in the transaction

	actions []action // actions performed in the current transaction, not comitted yet

	buffer   map[string][][]any // todo: buffer manager  mapping table->rows
	commited atomic.Bool
}

func newTransaction(d *delta) *transaction {
	tx := transactionPool.Get().(*transaction)
	tx.init(d)
	return tx
}

func (tx *transaction) init(d *delta) {
	tx.d = d
	tx.commited.Store(false)
	tx.tables = make(map[string]*table)
	tx.actions = make([]action, 0)
	tx.buffer = make(map[string][][]any)
	tx.id = 0
	// getPreviousLogs := func() []string {
	// 	all, err := tx.d.internalStorage.List("", logPrefix)
	// 	if err != nil {
	// 		slog.Error("error while searching for logs", slog.Any("error", err))
	// 		return nil
	// 	}
	// 	for _, a := range all {

	// 	}
	// 	return nil
	// }

	// previousLogs := getPreviousLogs()
}

func (tx *transaction) create(table string, columns []string) error {
	if _, ok := tx.tables[table]; ok {
		return errors.New("table exists")
	}
	tx.buffer[table] = make([][]any, 0)
	tx.tables[table] = newTable(table)

	cm := newChangeMetadaAction(table, columns)
	tx.actions = append(tx.actions, cm)

	return nil
}

func (tx *transaction) put(table string, values []any) error {
	// validate schema
	if _, ok := tx.tables[table]; !ok {
		return errors.New("table not found")
	}

	if tx.buffer[table] == nil {
		tx.buffer[table] = make([][]any, 0)
	}
	tx.buffer[table] = append(tx.buffer[table], values)
	return nil
}

func (tx *transaction) commit() error {
	defer transactionPool.Put(tx)

	if tx.d == nil {
		return errors.New("no delta conn")
	}

	if tx.commited.Load() {
		return errors.New("transaction already commited")
	}

	if len(tx.buffer) > 0 {
		// flush in-memory buffer to delta lake file and append add action
		err := tx.flushTables()
		if err != nil {
			return err
		}
	}

	if err := tx.logAndApply(); err != nil {
		return err
	}

	tx.commited.Store(true)
	return nil
}

func (tx *transaction) flushTable(name string) error {
	data, ok := tx.buffer[name]
	if !ok {
		return errors.New("table not found in memory")
	}

	do := &dataObject{
		Id:    uuid.NewString(),
		Table: name,
		Data:  data,
		Size:  len(data),
	}
	err := do.persist(tx.d.internalStorage)
	if err != nil {
		slog.Error("error while saving data object on disk", slog.String("table", name))
		return err
	}
	ao := newDataObjAction(do.Table, do.fileName, _add)
	tx.actions = append(tx.actions, ao)
	return nil
}

func (tx *transaction) flushTables() error {
	var err error
	for table, _ := range tx.buffer {
		if flushErr := tx.flushTable(table); flushErr != nil {
			err = multierr.Append(err, flushErr)
		}
	}
	return err
}

func (tx *transaction) logAndApply() error {
	var buf bytes.Buffer
	for _, a := range tx.actions {
		_, err := a.write(&buf)
		if err != nil {
			return err
		}
	}
	filname := fmt.Sprintf("%s_%d", logPrefix, tx.id)
	return tx.d.internalStorage.Write(filname, buf.Bytes())
}
