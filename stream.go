package wsstream

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/avast/retry-go/v4"

	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
)

type IBuild interface {
	Build(ctx context.Context, u *url.URL, h http.Header) (*url.URL, http.Header, error)
}

type IRetry interface {
	Retry(context.Context)
}

type IRelease interface {
	Release(*Connection)
}

type IAsync interface {
	Async(*Connection) error
}

type IOpen interface {
	Open(*Connection) error
}

type IMessage interface {
	Message(*Connection, int, []byte) error
}

type Stream struct {
	attempts  uint
	delay     time.Duration
	maxDelay  time.Duration
	heartbeat time.Duration
	conn      *Connection
	events    []any
}

func NewStream(attempts uint, delay, maxDelay, heartbeat time.Duration, events []any) *Stream {
	return &Stream{
		attempts:  attempts,
		delay:     delay,
		maxDelay:  maxDelay,
		heartbeat: heartbeat,
		events:    events,
	}
}

func (s *Stream) Start(ctx context.Context, rawURL string, options ...OptionsHandler) error {
	listener := NewListener()
	listener.SetReleaseHandler(func(conn *Connection) {
		lo.ForEach(lo.Reverse(s.events), func(e any, _ int) {
			impl, ok := e.(IRelease)
			if ok {
				impl.Release(conn)
			}
		})
	})
	listener.SetAsyncHandler(func(conn *Connection) error {
		g, ctx := errgroup.WithContext(conn)
		g.Go(func() error {
			ticker := time.NewTicker(s.heartbeat)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-ticker.C:
					if err := conn.WritePing(nil, time.Now().Add(s.heartbeat)); err != nil {
						return err
					}
				}
			}
		})
		lo.ForEach(s.events, func(event any, _ int) {
			g.Go(func() error {
				impl, ok := event.(IAsync)
				if !ok {
					return nil
				}
				return impl.Async(conn)
			})
		})
		return g.Wait()
	})
	listener.SetOpenHandler(func(conn *Connection) error {
		return lo.Reduce(s.events, func(err error, event any, _ int) error {
			if err != nil {
				return err
			}
			impl, ok := event.(IOpen)
			if !ok {
				return nil
			}
			return impl.Open(conn)
		}, nil)
	})
	listener.SetMessageHandler(func(conn *Connection, messageType int, data []byte) error {
		g, _ := errgroup.WithContext(conn)
		lo.ForEach(s.events, func(event any, _ int) {
			g.Go(func() error {
				impl, ok := event.(IMessage)
				if !ok {
					return nil
				}
				return impl.Message(conn, messageType, data)
			})
		})
		return g.Wait()
	})
	return WarpUnexpectedCloseError(retry.Do(
		func() error {
			u, err := url.Parse(rawURL)
			if err != nil {
				return err
			}
			h := http.Header{}
			for _, e := range s.events {
				impl, ok := e.(IBuild)
				if !ok {
					continue
				}
				u, h, err = impl.Build(ctx, u, h)
				if err != nil {
					return err
				}
			}
			return NewWebsocket(options...).Connect(ctx, u.String(), h, listener)
		},
		retry.Context(ctx),
		retry.Attempts(s.attempts),
		retry.MaxDelay(s.maxDelay),
		retry.Delay(s.delay),
		retry.OnRetry(func(n uint, err error) {
			lo.ForEach(lo.Reverse(s.events), func(e any, _ int) {
				impl, ok := e.(IRetry)
				if ok {
					impl.Retry(ctx)
				}
			})
		}),
		retry.DelayType(func(n uint, err error, config *retry.Config) time.Duration {
			return retry.BackOffDelay(n, err, config)
		}),
	))
}
