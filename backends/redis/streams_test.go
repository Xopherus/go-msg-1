package redis

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zerofox-oss/go-msg"
)

// $ brew upgrade redis
// $ redis-server -v
// Redis server v=7.2.5 sha=00000000:0 malloc=libc bits=64 build=bd81cd1340e80580
// Note, redis 7 is compatible with go-redis/v9, but not go-redis/v8
//

var redisUrl = "127.0.0.1:6379"

func Test_ServerSuccess(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: redisUrl})

	stream := "stream1"
	groups := []string{"group1", "group2"}

	// delete stream once test is complete
	cleanup := setupStream(t, streamConfig{
		Stream: stream,
		Groups: groups,
		Client: client,
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
	client := redis.NewClient(&redis.Options{Addr: redisUrl})

	stream := "stream2"
	groups := []string{"group1", "group2"}

	// delete stream once test is complete
	cleanup := setupStream(t, streamConfig{
		Stream: stream,
		Groups: groups,
		Client: client,
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

type streamConfig struct {
	Stream string
	Groups []string
	Client *redis.Client
}

func setupStream(t *testing.T, conf streamConfig) func() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Consumer groups should only receive new messages - therefore specify $ as the ID
	// See https://redis.io/docs/latest/commands/xgroup-create/
	for _, group := range conf.Groups {
		resp := conf.Client.XGroupCreateMkStream(ctx, conf.Stream, group, "$")
		if err := resp.Err(); err != nil {
			t.Fatalf("could not create consumer group (%s): %s", group, err)
		}
	}

	resp1 := conf.Client.XInfoStream(ctx, conf.Stream)
	if err := resp1.Err(); err != nil {
		t.Fatalf("XInfoStream failed with: %s", err)
	}

	// write messages to stream
	topic := Topic{Stream: conf.Stream, Conn: conf.Client}
	messages := []struct {
		Key   string
		Value string
	}{
		{
			Key:   "Hello",
			Value: "World",
		},
		{
			Key:   "This is",
			Value: "A message",
		},
	}

	for _, message := range messages {
		bytes, err := json.Marshal(message)
		if err != nil {
			t.Fatalf("could not marshal message: %s", err)
		}

		w := topic.NewWriter(ctx)
		if _, err := w.Write(bytes); err != nil {
			t.Fatalf("could not write message: %s", err)
		}

		if err := w.Close(); err != nil {
			t.Fatalf("could not close message: %s", err)
		}
	}

	t.Logf("Stream=%s has messages=%d, consumer_groups=%d", conf.Stream, len(messages), resp1.Val().Groups)

	return func() error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if err := conf.Client.Del(ctx, conf.Stream).Err(); err != nil {
			t.Errorf("DEL %s failed with: %s", conf.Stream, err)
			return err
		}

		return nil
	}
}
