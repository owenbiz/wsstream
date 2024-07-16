package wsstream

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"golang.org/x/time/rate"

	"github.com/samber/lo"

	"github.com/gorilla/websocket"
	"golang.org/x/sync/errgroup"
)

var (
	Upgrade = &websocket.Upgrader{
		HandshakeTimeout: 45 * time.Second,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
		EnableCompression: true,
	}

	Dialer = &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 45 * time.Second,
	}

	ErrClosed          = errors.New("websocket connection is closed")
	ErrAlreadyOpened   = errors.New("websocket connection is already opened")
	ErrWriteBufferFull = errors.New("connection write buffer is full")

	messagePool = &sync.Pool{
		New: func() any {
			return &message{}
		},
	}

	bufferPool = &sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 0, 1024))
		},
	}
)

const (
	statusClosed = 0
	statusOpened = 1
)

type IListener interface {
	OnRelease(conn *Connection)
	OnOpen(conn *Connection) error
	OnSend(conn *Connection, messageType int, data []byte) (int, []byte, error)
	OnMessage(conn *Connection, messageType int, data []byte) error
	OnClose(conn *Connection, code int, text string) error
	OnPing(conn *Connection, data string) error
	OnPong(conn *Connection, data string) error
	OnAsync(conn *Connection) error
}

type Listener struct {
	onReleaseHandler func(conn *Connection)
	onOpenHandler    func(conn *Connection) error
	onSendHandler    func(conn *Connection, messageType int, data []byte) (int, []byte, error)
	onMessageHandler func(conn *Connection, messageType int, data []byte) error
	onCloseHandler   func(conn *Connection, code int, text string) error
	onPingHandler    func(conn *Connection, data string) error
	onPongHandler    func(conn *Connection, data string) error
	onAsyncHandler   func(conn *Connection) error
}

func NewListener() *Listener {
	return &Listener{}
}

func (l *Listener) SetReleaseHandler(handler func(conn *Connection)) {
	l.onReleaseHandler = handler
}

func (l *Listener) OnRelease(conn *Connection) {
	if l.onReleaseHandler == nil {
		return
	}

	l.onReleaseHandler(conn)
}

func (l *Listener) SetOpenHandler(handler func(conn *Connection) error) {
	l.onOpenHandler = handler
}

func (l *Listener) OnOpen(conn *Connection) error {
	if l.onOpenHandler == nil {
		return nil
	}

	return l.onOpenHandler(conn)
}

func (l *Listener) SetSendHandler(handler func(conn *Connection, messageType int, data []byte) (int, []byte, error)) {
	l.onSendHandler = handler
}

func (l *Listener) OnSend(conn *Connection, messageType int, data []byte) (int, []byte, error) {
	if l.onSendHandler == nil {
		return messageType, data, nil
	}

	return l.onSendHandler(conn, messageType, data)
}

func (l *Listener) SetMessageHandler(handler func(conn *Connection, messageType int, data []byte) error) {
	l.onMessageHandler = handler
}

func (l *Listener) OnMessage(conn *Connection, messageType int, data []byte) error {
	if l.onMessageHandler == nil {
		return nil
	}

	return l.onMessageHandler(conn, messageType, data)
}

func (l *Listener) SetCloseHandler(handler func(conn *Connection, code int, text string) error) {
	l.onCloseHandler = handler
}

func (l *Listener) OnClose(conn *Connection, code int, text string) error {
	if l.onCloseHandler == nil {
		return conn.WriteClose(code, text, time.Now().Add(time.Second))
	}

	return l.onCloseHandler(conn, code, text)
}

func (l *Listener) SetPingHandler(handler func(conn *Connection, data string) error) {
	l.onPingHandler = handler
}

func (l *Listener) OnPing(conn *Connection, data string) error {
	if l.onPingHandler != nil {
		return l.onPingHandler(conn, data)
	}

	err := conn.WritePong([]byte(data), time.Now().Add(time.Second))
	if err != nil {
		if errors.Is(err, websocket.ErrCloseSent) {
			return nil
		}

		return err
	}

	return nil
}

func (l *Listener) SetPongHandler(handler func(conn *Connection, data string) error) {
	l.onPongHandler = handler
}

func (l *Listener) OnPong(conn *Connection, data string) error {
	if l.onPongHandler == nil {
		return nil
	}

	return l.onPongHandler(conn, data)
}

func (l *Listener) SetAsyncHandler(handler func(conn *Connection) error) {
	l.onAsyncHandler = handler
}

func (l *Listener) OnAsync(conn *Connection) error {
	if l.onAsyncHandler == nil {
		return nil
	}

	return l.onAsyncHandler(conn)
}

type message struct {
	messageType int
	data        []byte
}

func (m *message) reset(messageType int, data []byte) *message {
	m.messageType = messageType
	m.data = data
	return m
}

type Options struct {
	Logger       *zap.Logger
	ReadSize     int
	ReadLimiter  *rate.Limiter
	WriteSize    int
	WriteLimiter *rate.Limiter
}

type OptionsHandler func(o *Options)

func WithLogger(logger *zap.Logger) OptionsHandler {
	return func(o *Options) {
		o.Logger = logger
	}
}

func WithReadSize(size int) OptionsHandler {
	return func(o *Options) {
		o.ReadSize = size
	}
}

func WithReadLimiter(limiter *rate.Limiter) OptionsHandler {
	return func(o *Options) {
		o.ReadLimiter = limiter
	}
}

func WithWriteSize(size int) OptionsHandler {
	return func(o *Options) {
		o.WriteSize = size
	}
}

func WithWriteLimiter(limiter *rate.Limiter) OptionsHandler {
	return func(o *Options) {
		o.WriteLimiter = limiter
	}
}

type Connection struct {
	context.Context

	options      *Options
	status       atomic.Uint32
	conn         *websocket.Conn
	readChannel  chan *message
	writeChannel chan *message
}

func NewWebsocket(options ...OptionsHandler) *Connection {
	return &Connection{
		options: lo.Reduce(options, func(option *Options, warp OptionsHandler, _ int) *Options {
			warp(option)
			return option
		}, &Options{
			ReadSize:     1024,
			ReadLimiter:  nil,
			WriteSize:    1024,
			WriteLimiter: nil,
		}),
	}
}

func (c *Connection) open() {
	c.writeChannel = make(chan *message, c.options.WriteSize)
	c.readChannel = make(chan *message, c.options.ReadSize)
	c.status.Swap(statusOpened)
}

func (c *Connection) close() error {
	if c.isClosed() {
		return nil
	}
	c.status.Swap(statusClosed)
	close(c.readChannel)
	close(c.writeChannel)
	return c.conn.Close()
}

func (c *Connection) isOpened() bool {
	c.status.Load()
	return c.status.Load() == statusOpened
}

func (c *Connection) isClosed() bool {
	return c.status.Load() == statusClosed
}

type WriteOptions struct {
	Block  bool
	Direct bool
}

type WriteOptionsHandler func(o *WriteOptions)

func WriteWithBlock(o *WriteOptions) {
	o.Block = true
}

func WriteWithDirect(o *WriteOptions) {
	o.Direct = true
}

func (c *Connection) WriteJson(ctx context.Context, v any, options ...WriteOptionsHandler) error {
	return c.writeJsonWith(ctx, websocket.TextMessage, v, options...)
}

func (c *Connection) WriteBinaryJson(ctx context.Context, v any, options ...WriteOptionsHandler) error {
	return c.writeJsonWith(ctx, websocket.BinaryMessage, v, options...)
}

func (c *Connection) Write(ctx context.Context, data []byte, options ...WriteOptionsHandler) error {
	return c.writeMessageWith(ctx, websocket.TextMessage, data, options...)
}

func (c *Connection) WriteBinary(ctx context.Context, data []byte, options ...WriteOptionsHandler) error {
	return c.writeMessageWith(ctx, websocket.BinaryMessage, data, options...)
}

func (c *Connection) WriteString(ctx context.Context, data string, options ...WriteOptionsHandler) error {
	buffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buffer)

	buffer.Reset()
	buffer.WriteString(data)

	return c.writeMessageWith(ctx, websocket.TextMessage, buffer.Bytes(), options...)
}

func (c *Connection) WriteBinaryString(ctx context.Context, data string, options ...WriteOptionsHandler) error {
	buffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buffer)

	buffer.Reset()
	buffer.WriteString(data)

	return c.writeMessageWith(ctx, websocket.BinaryMessage, buffer.Bytes(), options...)
}

func (c *Connection) WriteJsonWith(ctx context.Context, messageType int, v any, options ...WriteOptionsHandler) error {
	return c.writeJsonWith(ctx, messageType, v, options...)
}

func (c *Connection) WriteMessageWith(ctx context.Context, messageType int, data []byte, options ...WriteOptionsHandler) error {
	return c.writeMessageWith(ctx, messageType, data, options...)
}

func (c *Connection) writeJsonWith(ctx context.Context, messageType int, v any, options ...WriteOptionsHandler) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return c.writeMessageWith(ctx, messageType, data, options...)
}

func (c *Connection) writeMessageWith(ctx context.Context, messageType int, data []byte, options ...WriteOptionsHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("write message panic %s", r)
		}
	}()

	if c.isClosed() {
		return ErrClosed
	}

	option := lo.Reduce(options, func(option *WriteOptions, warp WriteOptionsHandler, _ int) *WriteOptions {
		warp(option)
		return option
	}, &WriteOptions{})

	if option.Direct {
		return c.conn.WriteMessage(messageType, data)
	}

	if option.Block {
		select {
		case c.writeChannel <- messagePool.Get().(*message).reset(messageType, data):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	select {
	case c.writeChannel <- messagePool.Get().(*message).reset(messageType, data):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ErrWriteBufferFull
	}
}

func (c *Connection) WritePing(data []byte, deadline time.Time) error {
	return c.writeControl(websocket.PingMessage, data, deadline)
}

func (c *Connection) WritePong(data []byte, deadline time.Time) error {
	return c.writeControl(websocket.PongMessage, data, deadline)
}

func (c *Connection) WriteClose(closeCode int, text string, deadline time.Time) error {
	return c.writeControl(websocket.CloseMessage, websocket.FormatCloseMessage(closeCode, text), deadline)
}

func (c *Connection) WriteControl(messageType int, data []byte, deadline time.Time) error {
	return c.writeControl(messageType, data, deadline)
}

func (c *Connection) writeControl(messageType int, data []byte, deadline time.Time) error {
	if c.isClosed() {
		return ErrClosed
	}

	return c.conn.WriteControl(messageType, data, deadline)
}

func (c *Connection) SetWriteDeadline(deadline time.Time) error {
	if c.isClosed() {
		return ErrClosed
	}

	return c.conn.SetWriteDeadline(deadline)
}

func (c *Connection) SetReadDeadline(deadline time.Time) error {
	if c.isClosed() {
		return ErrClosed
	}

	return c.conn.SetReadDeadline(deadline)
}

func (c *Connection) SetDeadline(deadline time.Time) error {
	if c.isClosed() {
		return ErrClosed
	}

	return c.conn.NetConn().SetDeadline(deadline)
}

func (c *Connection) SetReadLimit(limit int64) error {
	if c.isClosed() {
		return ErrClosed
	}

	c.conn.SetReadLimit(limit)

	return nil
}

func (c *Connection) SetCompressionLevel(level int) error {
	if c.isClosed() {
		return ErrClosed
	}

	c.conn.EnableWriteCompression(level != flate.NoCompression)

	return c.conn.SetCompressionLevel(level)
}

func (c *Connection) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Connection) SubProtocol() string {
	return c.conn.Subprotocol()
}

func (c *Connection) log(level zapcore.Level, msg string, fields ...zap.Field) {
	if c.options.Logger != nil {
		c.options.Logger.Log(level, msg, fields...)
	}
}

func (c *Connection) run(ctx context.Context, conn *websocket.Conn, listener IListener) error {
	g, ctx := errgroup.WithContext(ctx)

	c.Context = ctx

	c.open()

	c.conn = conn

	c.conn.SetCloseHandler(func(code int, text string) error {
		return listener.OnClose(c, code, text)
	})

	c.conn.SetPingHandler(func(data string) error {
		return listener.OnPing(c, data)
	})

	c.conn.SetPongHandler(func(data string) error {
		return listener.OnPong(c, data)
	})

	defer func() {
		listener.OnRelease(c)
	}()

	err := func() error {
		return listener.OnOpen(c)
	}()
	if err != nil {
		return err
	}

	g.Go(func() error {
		return listener.OnAsync(c)
	})

	g.Go(func() error {
		<-c.Done()
		return c.close()
	})

	g.Go(func() error {
		for c.isOpened() && c.Err() == nil {
			if c.options.ReadLimiter != nil {
				if err := c.options.ReadLimiter.Wait(c); err != nil {
					return err
				}
			}

			messageType, data, err := c.conn.ReadMessage()
			if err != nil {
				return err
			}

			c.readChannel <- messagePool.Get().(*message).reset(messageType, data)
		}

		return nil
	})

	g.Go(func() error {
		fn := func(msg *message) error {
			defer messagePool.Put(msg)

			if c.options.WriteLimiter != nil {
				if err := c.options.WriteLimiter.Wait(c); err != nil {
					return err
				}
			}

			messageType, data, err := listener.OnSend(c, msg.messageType, msg.data)
			if err != nil {
				return err
			}

			return c.conn.WriteMessage(messageType, data)
		}

		for c.isOpened() {
			select {
			case <-c.Done():
				return nil
			case msg, ok := <-c.writeChannel:
				if !ok {
					return nil
				}

				err = fn(msg)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})

	g.Go(func() error {
		fn := func(msg *message) error {
			defer messagePool.Put(msg)

			return listener.OnMessage(c, msg.messageType, msg.data)
		}

		for c.isOpened() {
			select {
			case <-c.Done():
				return nil
			case msg, ok := <-c.readChannel:
				if !ok {
					return nil
				}

				err = fn(msg)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})

	return g.Wait()
}

func (c *Connection) Upgrade(ctx context.Context, response http.ResponseWriter, request *http.Request, header http.Header, listener IListener) error {
	if c.isOpened() {
		return ErrAlreadyOpened
	}

	conn, err := Upgrade.Upgrade(response, request, header)
	if err != nil {
		return err
	}

	return c.run(ctx, conn, listener)
}

func (c *Connection) Connect(ctx context.Context, url string, header http.Header, listener IListener) error {
	if c.isOpened() {
		return ErrAlreadyOpened
	}

	conn, resp, err := Dialer.DialContext(ctx, url, header)
	if err != nil {
		return err
	}

	defer func() { _ = resp.Body.Close() }()

	return c.run(ctx, conn, listener)
}

func (c *Connection) Start(ctx context.Context, conn *websocket.Conn, listener IListener) error {
	if c.isOpened() {
		return ErrAlreadyOpened
	}

	return c.run(ctx, conn, listener)
}

func WarpUnexpectedCloseError(err error) error {
	if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
		return nil
	} else {
		return err
	}
}
