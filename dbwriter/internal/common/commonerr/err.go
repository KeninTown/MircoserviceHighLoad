package commonerr

import "errors"

type Error struct {
	Err error `json:"err"`
}

func New(msg string) Error {
	return Error{errors.New(msg)}
}
