package main

const (
	_dataObjectSize = 1024
)

type dataObject struct {
	id string // generated uuid

	table string
	data  [_dataObjectSize][]any
}
