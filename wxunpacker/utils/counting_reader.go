package utils

import (
	"io"
)

type CountingReader struct {
    Reader io.Reader
    Bytes int64
}

func (cr *CountingReader) Read(p []byte) (int, error) {
    n, err := cr.Reader.Read(p)
    cr.Bytes += int64(n)
    return n, err
}

