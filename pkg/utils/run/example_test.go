package run

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"
)

func Test_ExampleGroup_Add_basic(t *testing.T) {
	var g Group
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			select {
			case <-time.After(time.Second):
				t.Logf("The first actor had its time elapsed\n")
				return nil
			case <-cancel:
				t.Logf("The first actor was canceled\n")
				return nil
			}
		}, func(err error) {
			t.Logf("The first actor was interrupted with: %v\n", err)
			close(cancel)
		})
	}
	{
		g.Add(func() error {
			t.Logf("The second actor is returning immediately\n")
			return errors.New("immediate teardown")
		}, func(err error) {
			// Note that this interrupt function is called, even though the
			// corresponding execute function has already returned.
			t.Logf("The second actor was interrupted with: %v\n", err)
		})
	}
	t.Logf("The group was terminated with: %v\n", g.Run())
	// Output:
	// The second actor is returning immediately
	// The first actor was interrupted with: immediate teardown
	// The second actor was interrupted with: immediate teardown
	// The first actor was canceled
	// The group was terminated with: immediate teardown
}

func Test_ExampleGroup_Add_context(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var g Group
	{
		ctx, cancel := context.WithCancel(ctx) // note: shadowed
		g.Add(func() error {
			return runUntilCanceled(ctx)
		}, func(error) {
			cancel()
		})
	}
	go cancel()
	t.Logf("The group was terminated with: %v\n", g.Run())
	// Output:
	// The group was terminated with: context canceled
}

func Test_ExampleGroup_Add_listener(t *testing.T) {
	var g Group
	{
		ln, _ := net.Listen("tcp", ":0")
		g.Add(func() error {
			defer t.Logf("http.Serve returned\n")
			return http.Serve(ln, http.NewServeMux())
		}, func(error) {
			ln.Close()
		})
	}
	{
		g.Add(func() error {
			return errors.New("immediate teardown")
		}, func(error) {
			//
		})
	}
	t.Logf("The group was terminated with: %v\n", g.Run())
	// Output:
	// http.Serve returned
	// The group was terminated with: immediate teardown
}

func runUntilCanceled(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}
