package rabbitmq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/zerofox-oss/go-msg"
)

type Server struct {
	Queue       string
	Concurrency int

	conn          *amqp.Connection
	inFlightQueue chan struct{}

	// context used to shutdown processing of in-flight messages
	receiverCtx        context.Context
	receiverCancelFunc context.CancelFunc

	// context used to shutdown the server
	serverCtx        context.Context
	serverCancelFunc context.CancelFunc
}

func (s *Server) Serve(r msg.Receiver) error {
	ch, err := s.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	notifyConnClose := s.conn.NotifyClose(make(chan *amqp.Error, 1))
	notifyChanClose := ch.NotifyClose(make(chan *amqp.Error, 1))

	// open queue channel
	msgChan, err := ch.Consume(s.Queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	for {
		select {
		case <-s.serverCtx.Done():
			ch.Close()
			close(s.inFlightQueue)

			return msg.ErrServerClosed

		// exit if conn or channel are closed
		case err, ok := <-notifyConnClose:
			if ok {
				return fmt.Errorf("amqp connection was closed: %s, exiting", err)
			}

		case err, ok := <-notifyChanClose:
			if ok {
				return fmt.Errorf("amqp channel was closed: %s", err)
			}

		case m := <-msgChan:
			// apparently this doesn't block if there's no messages in the queue, it just returns an empty message
			if m.Acknowledger == nil {
				continue
			}

			s.inFlightQueue <- struct{}{}

			go func(d amqp.Delivery) {
				defer func() {
					<-s.inFlightQueue
				}()

				if err := s.process(d, r); err != nil {
					log.Printf("failed to process: %s", err)
				}
			}(m)
		}
	}
}

func (s *Server) process(d amqp.Delivery, r msg.Receiver) error {
	message := msg.Message{
		Body: bytes.NewBuffer(d.Body),
	}

	if err := r.Receive(s.receiverCtx, &message); err != nil {
		if err := d.Nack(false, true); err != nil {
			return err
		}

		throttleErr, ok := err.(msg.ErrServerThrottled)
		if ok {
			time.Sleep(throttleErr.Duration)
			return nil
		}

		return fmt.Errorf("receiver error: %w", err)
	}

	if err := d.Ack(false); err != nil {
		return fmt.Errorf("could not ACK message: %w", err)
	}

	return nil
}

const shutdownPollInterval = 500 * time.Millisecond

// Shutdown stops the receipt of new messages and waits for routines
// to complete or the passed in ctx to be canceled. msg.ErrServerClosed
// will be returned upon a clean shutdown. Otherwise, the passed ctx's
// Error will be returned.
func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		panic("context not set")
	}

	s.serverCancelFunc()

	ticker := time.NewTicker(shutdownPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.receiverCancelFunc()

			return ctx.Err()
		case <-ticker.C:
			if len(s.inFlightQueue) == 0 {
				return msg.ErrServerClosed
			}
		}
	}
}

// Option is the signature that modifies a `Server` to set some configuration
type Option func(*Server) error

func NewServer(conn *amqp.Connection, queue string, opts ...Option) (*Server, error) {
	defaultConcurrency := 10

	serverCtx, serverCancelFunc := context.WithCancel(context.Background())
	receiverCtx, receiverCancelFunc := context.WithCancel(context.Background())

	srv := &Server{
		Queue:       queue,
		Concurrency: defaultConcurrency,

		conn:          conn,
		inFlightQueue: make(chan struct{}, defaultConcurrency),

		receiverCtx:        receiverCtx,
		receiverCancelFunc: receiverCancelFunc,
		serverCtx:          serverCtx,
		serverCancelFunc:   serverCancelFunc,
	}

	for _, opt := range opts {
		if err := opt(srv); err != nil {
			return nil, err
		}
	}

	return srv, nil
}

func WithConcurrency(c int) func(*Server) error {
	return func(srv *Server) error {
		srv.Concurrency = c
		srv.inFlightQueue = make(chan struct{}, c)

		return nil
	}
}

// Topic publishes Messages to a RabbitMQ Exchange.
type Topic struct {
	Exchange string
	Conn     *amqp.Connection
}

// NewWriter returns a MessageWriter.
// The MessageWriter may be used to write messages to a RabbitMQ Exchange.
func (t *Topic) NewWriter(ctx context.Context) msg.MessageWriter {
	return &MessageWriter{
		ctx:        ctx,
		attributes: make(map[string][]string),
		buf:        &bytes.Buffer{},

		exchange: t.Exchange,
		conn:     t.Conn,
	}
}

// MessageWriter is used to publish a single Message to a RabbitMQ Exchange.
// Once all of the data has been written and closed, it may not be used again.
type MessageWriter struct {
	msg.MessageWriter

	exchange string
	conn     *amqp.Connection

	ctx        context.Context
	attributes msg.Attributes
	buf        *bytes.Buffer // internal buffer
	closed     bool
	mux        sync.Mutex
}

// Attributes returns the attributes of the MessageWriter.
func (w *MessageWriter) Attributes() *msg.Attributes {
	return &w.attributes
}

// Write writes bytes to an internal buffer.
func (w *MessageWriter) Write(p []byte) (int, error) {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return 0, msg.ErrClosedMessageWriter
	}
	return w.buf.Write(p)
}

// Close publishes a Message.
// If the MessageWriter is already closed it will return an error.
func (w *MessageWriter) Close() error {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return msg.ErrClosedMessageWriter
	}
	w.closed = true

	if w.buf.Len() > 0 {
		raw, err := json.Marshal(w.buf)
		if err != nil {
			return err
		}

		ch, err := w.conn.Channel()
		if err != nil {
			return err
		}
		defer ch.Close()

		return ch.PublishWithContext(w.ctx, w.exchange, "", false, false, amqp.Publishing{
			Headers: attrsToHeaders(w),
			Body:    raw,
		})
	}

	return nil
}

func attrsToHeaders(w msg.MessageWriter) map[string]interface{} {
	headers := map[string]interface{}{}

	for k, v := range *w.Attributes() {
		headers[k] = v
	}

	return headers
}
