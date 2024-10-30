package deltalake

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

const (
	_testDir = "./_test"
)

func init() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
}

func cleanup(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
}

func getTestDir() string {
	uid := uuid.NewString()
	return path.Join(_testDir, uid)
}

func TestTransaction(t *testing.T) {
	testdir := getTestDir()
	objStorage := newFileStorage(testdir)
	cl := New(objStorage, DefaultOpts())
	tx := cl.NewTransaction()
	assert.NoError(t, tx.create("foo", []string{"name1", "name2", "val1", "val2"}))
	assert.NoError(t, tx.put("foo", []any{"foo1", "bar", 1, 2}))
	assert.NoError(t, tx.put("foo", []any{"foo1", "bar", 1, 2}))
	assert.NoError(t, tx.put("foo", []any{"foo1", "bar", 1, 2}))
	assert.NoError(t, tx.put("foo", []any{"foo1", "bar", 1, 2}))
	assert.NoError(t, tx.commit())

	txRead := cl.NewTransaction()
	_ = txRead

	assert.NoError(t, txRead.commit())

	cleanup(testdir)
}

func TestTransactionReadCommited(t *testing.T) {
	testdir := getTestDir()
	objStorage := newFileStorage(testdir)
	cl := New(objStorage, DefaultOpts())
	tx := cl.NewTransaction()
	assert.NoError(t, tx.create("foo", []string{"name1", "name2", "val1", "val2"}))

	for i := 0; i <= 100; i++ {
		assert.NoError(t, tx.put("foo", []any{
			fmt.Sprintf("foo%d", i),
			fmt.Sprintf("bar%d", i+20),
			i, i + 100,
		}))
	}

	assert.NoError(t, tx.commit())

	txRead := cl.NewTransaction()
	it, err := txRead.Iter("foo")
	assert.NoError(t, err)
	val, err := it.First()
	assert.NoError(t, err)
	for i := 0; i <= 100; i++ {
		assert.Len(t, val, 4)
		assert.Equal(t, fmt.Sprintf("foo%d", i), val[0])
		assert.Equal(t, fmt.Sprintf("bar%d", i+20), val[1])
		assert.Equal(t, float64(i), val[2])
		assert.Equal(t, float64(i+100), val[3])
		val, err = it.Next()
		if errors.Is(err, ErrIteratorExhausted) {
			return
		}
		assert.NoError(t, err)
	}

	cleanup(testdir)
}
