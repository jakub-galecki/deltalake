package deltalake

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

	actions []action // todo: make actions on specific table

	buffer   bytes.Buffer // todo: buffer manager  mapping table->rows
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
}

func (tx *transaction) put(table string, values []any) error {
	// validate schema

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

	if tx.buffer.Len() > 0 {
		// flush in-memory buffer to delta lake file and append add action

	}

	if err := tx.logAndApply(); err != nil {
		return err
	}

	tx.commited.Store(true)
	return nil
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
