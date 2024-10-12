package deltalake

type table struct {
	name  string
	files []string // underlying table files

	data    []any
	actions []action

	dirty bool // if any data was changed in the transaction
}
