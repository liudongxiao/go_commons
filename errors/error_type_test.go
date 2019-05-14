package errors

import (
	"context"
	"testing"
	"time"
)

func TestReRunErr(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"nil", args{err: nil}, false},
		{"context cancel", args{err: getTimeoutErr()}, true},
		{"tag zero ", args{err: getZero()}, false},
		{"tag zero wrap", args{err: Wrap(getZero(), "")}, false},
		{"error wrap ", args{err: getWrapErr(getTimeoutErr())}, true},
		{"error wrap Casuse context", args{err: Casuse(getWrapErr(getTimeoutErr()))}, true},
		{"error wrap Casuse zero ", args{err: Casuse(getWrapErr(getZero()))}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if got := ReRunErr(tt.args.err); got != tt.want {
				t.Errorf("ReRunErr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func getTimeoutErr() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	select {
	case <-ctx.Done():
		return ctx.Err()
	}
}

func getZero() error {
	return ErrTagZero{1}
}

func getWrapErr(err error) error {
	return Wrap(err, "")

}
