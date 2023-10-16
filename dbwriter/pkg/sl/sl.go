package sl

import "github.com/sagikazarmark/slog-shim"

func Error(err error) slog.Attr {
	return slog.Attr{
		Key:   "err",
		Value: slog.StringValue(err.Error()),
	}
}
