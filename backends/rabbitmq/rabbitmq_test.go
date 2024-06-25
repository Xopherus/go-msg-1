package rabbitmq

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/zerofox-oss/go-msg"
)

const rabbitmqURL = "amqp://guest:guest@localhost:5672"

func Test_RabbitMQ(t *testing.T) {
	conn, err := amqp.Dial(rabbitmqURL)
	if err != nil {
		t.Fatalf("could not connect to rabbitmq: %s", err)
	}
	defer conn.Close()

	t.Log("opened connection")

	// creates a pub/sub exchange
	conf := exchangeConf{
		Name:        "my_new_exchange",
		Type:        amqp.ExchangeTopic,
		Queues:      []string{"queue1"},
		NumMessages: 1000000,
	}

	cleanup := setupExchange(t, conn, conf)
	defer cleanup()

	// setup consumers for each group
	var count atomic.Uint32

	go func() {
		srvConn, err := amqp.Dial(rabbitmqURL)
		if err != nil {
			t.Errorf("could not connect to rabbitmq: %s", err)
		}
		defer srvConn.Close()

		srv1, err := NewServer(srvConn, conf.Queues[0])
		if err != nil {
			t.Errorf("could not start server: %s", err)
		}

		receiveFunc := msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
			t.Logf("Returning without error to ACK.")
			count.Add(1)

			return nil
		})

		// sleep to allow the loop below to start
		time.Sleep(1 * time.Second)
		srv1.Serve(receiveFunc)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			if actual := count.Load(); actual != uint32(conf.NumMessages) {
				t.Errorf("Expected %d messages to be processed, got %d", conf.NumMessages, actual)
			}

			t.Fail()
			return
		default:
			// inspect state of exchange
			time.Sleep(1 * time.Second)
		}
	}
}

type exchangeConf struct {
	Name        string
	Type        string
	Queues      []string
	NumMessages int
}

func setupExchange(t *testing.T, conn *amqp.Connection, conf exchangeConf) func() error {
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("could not open channel: %s", err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare(conf.Name, conf.Type, true, false, false, false, nil); err != nil {
		t.Errorf("could not declare exchange: %s", err)
	}

	// setup queues
	for _, name := range conf.Queues {
		_, err := ch.QueueDeclare(name, true, false, false, false, nil)
		if err != nil {
			t.Fatalf("could not create queue: %s", err)
		}

		if err := ch.QueueBind(name, "#", conf.Name, false, nil); err != nil {
			t.Fatalf("could not bind queue: %s", err)
		}
	}

	// publish a bunch of messages to the exchange
	topic := Topic{Conn: conn, Exchange: conf.Name}
	batches := make(chan struct{}, 10)
	batchSize := 1000
	totalWrites := 0

	for i := 0; i < conf.NumMessages/batchSize; i++ {
		if totalWrites%(batchSize*10) == 0 {
			t.Logf("Wrote %d messages to exchange", totalWrites)
		}

		batches <- struct{}{}

		go func() {
			// generate random string once, because this isn't safe to generate in goroutines
			var r *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

			defer func() {
				<-batches
			}()

			for j := 0; j < batchSize; j++ {
				// generate random string of 100KB
				raw, err := json.Marshal(String(r, 100000))
				if err != nil {
					t.Errorf("could not marshal message: %s", err)
				}

				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()

				w := topic.NewWriter(ctx)
				if _, err := w.Write(raw); err != nil {
					t.Errorf("could not write message: %s", err)
				}

				if err := w.Close(); err != nil {
					t.Errorf("could not close message: %s", err)
				}
			}

			totalWrites += batchSize
		}()
	}

	t.Logf("Exchange=%s configured with queues=%d, messages=%d", conf.Name, len(conf.Queues), conf.NumMessages)

	return func() error {
		ch, err := conn.Channel()
		if err != nil {
			t.Fatalf("could not open channel: %s", err)
		}
		defer ch.Close()

		for _, name := range conf.Queues {
			if _, err := ch.QueueDelete(name, false, false, false); err != nil {
				t.Errorf("could not delete queue %s: %s", name, err)
			}
		}

		if err := ch.ExchangeDelete(conf.Name, false, false); err != nil {
			t.Errorf("could not delete exchange: %s", err)
		}

		return err
	}
}

// for testing stream sizes
// https://www.calhoun.io/creating-random-strings-in-go/
const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func StringWithCharset(r *rand.Rand, length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[r.Intn(len(charset))]
	}
	return string(b)
}

func String(r *rand.Rand, length int) string {
	return StringWithCharset(r, length, charset)
}
