package handlers

type Error struct {
	Err string `json:"err"`
}

func NewResponseErr(msg string) Error {
	return Error{msg}
}
