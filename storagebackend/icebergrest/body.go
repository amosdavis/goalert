package icebergrest

import (
	"context"
	"io"
)

type body struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *body) Close() error {
	defer b.cancel()
	return b.ReadCloser.Close()
}
