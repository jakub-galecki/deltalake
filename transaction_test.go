package deltalake

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransaction(t *testing.T) {
	objStorage := newFileStorage("./test")
	cl := New(objStorage)

	tx := cl.NewTransaction()
	tx.create("foo", []string{"name1", "name2", "val1", "val2"})
	tx.put("foo", []any{"foo1", "bar", 1, 2})
	tx.put("foo", []any{"foo1", "bar", 1, 2})
	tx.put("foo", []any{"foo1", "bar", 1, 2})
	tx.put("foo", []any{"foo1", "bar", 1, 2})
	assert.NoError(t, tx.commit())
}
