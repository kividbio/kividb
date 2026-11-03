// KiviDB with go-redis: strings, expiry, hashes, a pipeline and a
// transaction.
//
//	go run .                                   # KiviDB on localhost:6380
//	KIVIDB_URL=redis://:secret@host:6380/0 go run .
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	url := os.Getenv("KIVIDB_URL")
	if url == "" {
		url = "redis://localhost:6380/0"
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		log.Fatal(err)
	}
	rdb := redis.NewClient(opts)
	defer rdb.Close()
	ctx := context.Background()

	must(rdb.Ping(ctx).Err())

	// Start clean: these are the keys this example uses.
	must(rdb.Del(ctx, "greeting", "session:42", "user:1", "balance").Err())

	// Strings, with and without an expiry.
	must(rdb.Set(ctx, "greeting", "hello", 0).Err())
	expect(rdb.Get(ctx, "greeting").Val(), "hello")
	must(rdb.Set(ctx, "session:42", "alice", time.Minute).Err())
	if ttl := rdb.TTL(ctx, "session:42").Val(); ttl <= 0 || ttl > time.Minute {
		log.Fatalf("unexpected TTL %v", ttl)
	}

	// Hashes.
	must(rdb.HSet(ctx, "user:1", "name", "Alice", "plan", "pro").Err())
	expect(rdb.HGet(ctx, "user:1", "plan").Val(), "pro")

	// A pipeline: many commands, one round trip.
	pipe := rdb.Pipeline()
	for i := 0; i < 100; i++ {
		pipe.Set(ctx, fmt.Sprintf("item:%d", i), i, 0)
	}
	_, err = pipe.Exec(ctx)
	must(err)
	expect(rdb.Get(ctx, "item:99").Val(), "99")

	// A transaction: MULTI/EXEC.
	must(rdb.Set(ctx, "balance", 100, 0).Err())
	var decr *redis.IntCmd
	_, err = rdb.TxPipelined(ctx, func(tx redis.Pipeliner) error {
		decr = tx.DecrBy(ctx, "balance", 30)
		return nil
	})
	must(err)
	if decr.Val() != 70 {
		log.Fatalf("balance %d, want 70", decr.Val())
	}

	fmt.Println("go quickstart: ok")
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func expect(got, want string) {
	if got != want {
		log.Fatalf("got %q, want %q", got, want)
	}
}
