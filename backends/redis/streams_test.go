package redis

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zerofox-oss/go-msg"
)

var redisURL = "localhost:6379"

func Test_Redis(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: redisURL})

	stream := "stream1"
	groups := []string{"group1", "group2"}

	// delete stream once test is complete
	cleanup := setupStream(t, client, streamConf{
		Stream:      stream,
		Groups:      groups,
		NumMessages: 1000000,
	})
	defer cleanup()

	// setup consumers for each group
	var count atomic.Uint32

	go func() {
		srv1, err := NewServer(client, stream, "group1", "consumer1")
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
			if actual := count.Load(); actual != uint32(2) {
				t.Errorf("Expected 2 messages to be processed, got %d", actual)
			}

			return
		default:
			resp := client.XInfoGroups(ctx, stream)
			if err := resp.Err(); err != nil {
				t.Errorf("XInfoGroups failed with: %s", err)
			}

			t.Logf("XInfoGroups: %+v", resp.Val())
			time.Sleep(1 * time.Second)
		}
	}
}

func Test_ServerClaimPendingMessages(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: redisURL})

	stream := "stream2"
	groups := []string{"group1", "group2"}

	// delete stream once test is complete
	cleanup := setupStream(t, client, streamConf{
		Stream:      stream,
		Groups:      groups,
		NumMessages: 100,
	})
	defer cleanup()

	// setup consumers for each group
	var count atomic.Uint32

	// srv1 - fails to process messages
	go func() {
		srv1, err := NewServer(client, stream, "group1", "consumer1")
		if err != nil {
			t.Errorf("could not start server: %s", err)
			return
		}

		receiveFunc := msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
			t.Logf("Simulating a message failure in server1, returning error to re-assign")

			return errors.New("message timed out")
		})

		if err := srv1.Serve(receiveFunc); err != nil {
			t.Logf("srv1 failed with: %s", err)
		}
	}()

	// srv2 - starts a few seconds after srv1 to let srv1 claim both messages.
	go func() {
		srv2, err := NewServer(client, stream, "group1", "consumer2")
		if err != nil {
			t.Errorf("could not start server: %s", err)
			return
		}

		time.Sleep(1 * time.Second)

		receiveFunc := msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
			t.Logf("Returning without error to ACK.")

			count.Add(1)

			return nil
		})

		if err := srv2.Serve(receiveFunc); err != nil {
			t.Logf("srv2 failed with: %s", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			if actual := count.Load(); actual != uint32(2) {
				t.Errorf("Expected 2 messages to be processed, got %d", actual)
			}

			return
		default:
			resp, err := client.XInfoGroups(ctx, stream).Result()
			if err != nil {
				t.Errorf("XInfoGroups failed with: %s", err)
			}

			t.Logf("XInfoGroups: %+v", resp)
			time.Sleep(1 * time.Second)
		}
	}
}

type streamConf struct {
	Stream      string
	Groups      []string
	NumMessages int
}

func setupStream(t *testing.T, client *redis.Client, conf streamConf) func() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Consumer groups should only receive new messages - therefore specify $ as the ID
	// See https://redis.io/docs/latest/commands/xgroup-create/
	for _, group := range conf.Groups {
		resp := client.XGroupCreateMkStream(ctx, conf.Stream, group, "$")
		if err := resp.Err(); err != nil {
			t.Fatalf("could not create consumer group (%s): %s", group, err)
		}
	}

	// publish a bunch of messages to the stream
	topic := Topic{Stream: conf.Stream, Conn: client}
	batches := make(chan struct{}, 10)
	batchSize := 1000
	totalWrites := 0

	for i := 0; i < conf.NumMessages/batchSize; i++ {
		if totalWrites%(batchSize*10) == 0 {
			t.Logf("Wrote %d messages to stream", totalWrites)
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

	t.Logf("Stream=%s configured with consumer_groups=%d, messages=%d", conf.Stream, len(conf.Groups), conf.NumMessages)

	return func() error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if err := client.Del(ctx, conf.Stream).Err(); err != nil {
			t.Errorf("DEL %s failed with: %s", conf.Stream, err)
			return err
		}

		return nil
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
