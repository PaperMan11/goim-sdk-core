package run

import (
	"errors"

	"testing"

	"github.com/PaperMan11/goim-sdk-core/pkg/utils/timex"
)

func TestZero(t *testing.T) {
	var g Group
	res := make(chan error)
	go func() { res <- g.Run() }()
	select {
	case err := <-res:
		if err != nil {
			t.Errorf("%v", err)
		}
		t.Log("success")
	case <-timex.After(100 * timex.Millisecond):
		t.Error("timeout")
	}
}

func TestOne(t *testing.T) {
	myError := errors.New("foobar")
	var g Group
	g.Add(func() error { return myError }, func(error) {})
	res := make(chan error)
	go func() { res <- g.Run() }()
	select {
	case err := <-res:
		if want, have := myError, err; want != have {
			t.Errorf("want %v, have %v", want, have)
		}
	case <-timex.After(100 * timex.Millisecond):
		t.Error("timeout")
	}
}

func TestMany(t *testing.T) {
	interrupt := errors.New("interrupt")
	var g Group
	g.Add(func() error { return interrupt }, func(error) {})
	cancel := make(chan struct{})
	g.Add(func() error { <-cancel; return nil }, func(error) { close(cancel) })
	res := make(chan error)
	go func() { res <- g.Run() }()
	select {
	case err := <-res:
		if want, have := interrupt, err; want != have {
			t.Errorf("want %v, have %v", want, have)
		}
	case <-timex.After(100 * timex.Millisecond):
		t.Errorf("timeout")
	}
}
