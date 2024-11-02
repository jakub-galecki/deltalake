package deltalake

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
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

	getTxId := func(name string) int64 {
		raw, found := strings.CutPrefix(name, "_log_")
		if !found {
			return 0
		}
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return 0
		}
		return id
	}

	previousLogs := func() []action {
		all, err := tx.d.internalStorage.List("", logPrefix)
		if err != nil {
			slog.Error("error while searching for logs", slog.Any("error", err))
			return nil
		}

		l := newLogs()
		actions := make([]action, 0)
		for _, a := range all {
			tx.id = getTxId(a) + 1
			slog.Debug("processing previous log", slog.String("id", a))
			reader, err := tx.d.internalStorage.Read(a)
			if err != nil {
				slog.Error("error while getting log file", slog.String("id", a), slog.Any("error", err))
				return nil
			}
			raw, err := io.ReadAll(reader)
			if err != nil {
				slog.Error("error while reading log file", slog.String("id", a), slog.Any("error", err))
				return nil
			}
			l, err := l.deserialize(raw)
			if err != nil {
				slog.Error("error while deserializing log", slog.String("id", a), slog.Any("error", err))
				return nil
			}
			acs, err := l.actions()
			if err != nil {
				slog.Error("error while deserializing actions", slog.String("id", a), slog.Any("error", err))
				return nil
			}
			actions = append(actions, acs...)
		}
		return actions
	}()

	// build tables based on previous logs

	builders := make(map[string]*tableBuilder)
	for _, a := range previousLogs {
		t := a.getTable()
		tb, ok := builders[t]
		if !ok {
			builders[t] = newTableBuilder(t, tx.d.internalStorage)
			tb = builders[t]
		}
		tb.add(a)
	}

	for t, tb := range builders {
		tx.tables[t] = tb.build()
	}
}

func (tx *transaction) Create(table string, columns []string) error {
	if _, ok := tx.tables[table]; ok {
		return errors.New("table exists")
	}
	tx.buffer[table] = make([][]any, 0)
	tx.tables[table] = newTable(table, tx.d.internalStorage)

	cm := newChangeMetadaAction(table, columns)
	tx.actions = append(tx.actions, cm)

	return nil
}

func (tx *transaction) Put(table string, values []any) error {
	// validate schema
	if _, ok := tx.tables[table]; !ok {
		return errors.New("table not found")
	}

	if tx.buffer[table] == nil {
		tx.buffer[table] = make([][]any, 0)
	}

	if len(tx.buffer[table]) == tx.d.opts.MaxMemoryBufferSz {
		// todo: make async
		if err := tx.flushTable(table); err != nil {
			return err
		}
	}

	tx.buffer[table] = append(tx.buffer[table], values)
	return nil
}

func (tx *transaction) Commit() error {
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

	// todo: add table to transaction cache
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
	tx.buffer[name] = tx.buffer[name][:0]
	return nil
}

func (tx *transaction) flushTables() error {
	var err error
	for table := range tx.buffer {
		if flushErr := tx.flushTable(table); flushErr != nil {
			err = multierr.Append(err, flushErr)
		}
	}
	return err
}

func (tx *transaction) logAndApply() error {
	l := newLogs()
	for _, a := range tx.actions {
		le, err := newLogEntry(a)
		if err != nil {
			return err
		}
		l = l.append(le)
	}
	filname := fmt.Sprintf("%s_%d", logPrefix, tx.id)
	rawLogs, err := l.serialize()
	if err != nil {
		return err
	}
	return tx.d.internalStorage.Write(filname, rawLogs)
}

func (tx *transaction) Iter(name string) (Iterator, error) {
	table, ok := tx.tables[name]
	if !ok {
		return nil, errors.New("table does not exist")
	}
	return table.scan(), nil
}
