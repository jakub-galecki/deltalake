package deltalake

type DeltaStorage interface {
	NewTransaction() *Transaction
}

type Iterator interface {
	First() ([]any, error)
	Next() ([]any, error)
}

type delta struct {
	internalStorage ObjectStorage

	opts *Opts
	// todo: table cache
}

func New(objstorage ObjectStorage, opt *Opts) DeltaStorage {
	return &delta{
		internalStorage: objstorage,
		opts:            opt,
	}
}

func (d *delta) NewTransaction() *Transaction {
	return newTransaction(d)
}

type Opts struct {
	MaxMemoryBufferSz int
}

func DefaultOpts() *Opts {
	return &Opts{
		MaxMemoryBufferSz: 10000,
	}
}
