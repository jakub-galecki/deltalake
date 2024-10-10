package deltalake

type DeltaStorage interface {
	NewTransaction() *transaction
}

type delta struct {
	internalStorage ObjectStorage
}

func New(objstorage ObjectStorage) DeltaStorage {
	return &delta{
		internalStorage: objstorage,
	}
}

func (d *delta) NewTransaction() *transaction {
	return newTransaction(d)
}

func (d *delta) getActions() {

}
