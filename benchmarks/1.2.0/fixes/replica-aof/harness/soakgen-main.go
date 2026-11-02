// soakgen: a mixed Redis-protocol workload for long soak runs.
//
// Strings (small and large values, some with expiries), hashes, lists, sets,
// sorted sets, counters, EXPIRE and DEL, over bounded key spaces, so the data
// set reaches a steady size and memory should then stay flat. Every 10 s it
// prints one CSV line: elapsed seconds, operations, errors, p50 / p99 / p99.9
// / max latency in microseconds of that interval (HDR histogram).
//
//	soakgen -addr 10.0.0.4:6379 -conns 64 -rate 80000 -duration 3h > soak.csv
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/redis/go-redis/v9"
)

var (
	addr     = flag.String("addr", "127.0.0.1:6379", "server")
	conns    = flag.Int("conns", 64, "connections (one goroutine each)")
	rate     = flag.Int("rate", 0, "total ops/s target (0 = unthrottled)")
	duration = flag.Duration("duration", time.Minute, "run time")
	interval = flag.Duration("interval", 10*time.Second, "sampling interval")
	strKeys  = flag.Int("str-keys", 1_000_000, "small string keys")
	bigKeys  = flag.Int("big-keys", 20_000, "large string keys (2-32 KB)")
	colKeys  = flag.Int("col-keys", 20_000, "keys per collection type")
	seed     = flag.Int64("seed", 1, "random seed")
	prefill  = flag.Bool("prefill", false, "write every string key once first, then exit")
	opStats  = flag.Bool("op-stats", false, "print per-operation service time max and count over 10 ms to stderr each interval")
)

const pad = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

var bigBuf []byte

func val(r *rand.Rand, n int) string {
	b := make([]byte, n)
	off := r.Intn(len(pad))
	for i := range b {
		b[i] = pad[(off+i)%len(pad)]
	}
	return string(b)
}

type op func(ctx context.Context, c *redis.Client, r *rand.Rand) error

func ops() ([]op, []int) {
	k := func(p string, n int, r *rand.Rand) string { return p + strconv.Itoa(r.Intn(n)) }
	list := []struct {
		w int
		f op
	}{
		{28, func(ctx context.Context, c *redis.Client, r *rand.Rand) error { // GET small
			err := c.Get(ctx, k("s:", *strKeys, r)).Err()
			if err == redis.Nil {
				return nil
			}
			return err
		}},
		{14, func(ctx context.Context, c *redis.Client, r *rand.Rand) error { // SET small, 64-512 B
			return c.Set(ctx, k("s:", *strKeys, r), val(r, 64+r.Intn(448)), 0).Err()
		}},
		{3, func(ctx context.Context, c *redis.Client, r *rand.Rand) error { // SET with expiry 5-300 s
			return c.Set(ctx, k("e:", *strKeys/4, r), val(r, 64+r.Intn(192)), time.Duration(5+r.Intn(296))*time.Second).Err()
		}},
		{1, func(ctx context.Context, c *redis.Client, r *rand.Rand) error { // SET large, 2-32 KB
			n := 2048 + r.Intn(30720)
			off := r.Intn(len(bigBuf) - n)
			return c.Set(ctx, k("big:", *bigKeys, r), bigBuf[off:off+n], 0).Err()
		}},
		{2, func(ctx context.Context, c *redis.Client, r *rand.Rand) error { // GET large
			err := c.Get(ctx, k("big:", *bigKeys, r)).Err()
			if err == redis.Nil {
				return nil
			}
			return err
		}},
		{8, func(ctx context.Context, c *redis.Client, r *rand.Rand) error { // HSET
			return c.HSet(ctx, k("h:", *colKeys, r), "f"+strconv.Itoa(r.Intn(50)), val(r, 16+r.Intn(48))).Err()
		}},
		{5, func(ctx context.Context, c *redis.Client, r *rand.Rand) error { // HGET / HGETALL
			if r.Intn(4) == 0 {
				return c.HGetAll(ctx, k("h:", *colKeys, r)).Err()
			}
			err := c.HGet(ctx, k("h:", *colKeys, r), "f"+strconv.Itoa(r.Intn(50))).Err()
			if err == redis.Nil {
				return nil
			}
			return err
		}},
		{6, func(ctx context.Context, c *redis.Client, r *rand.Rand) error { // LPUSH + LTRIM (lists stay <= 100)
			key := k("l:", *colKeys, r)
			p := c.Pipeline()
			p.LPush(ctx, key, val(r, 16+r.Intn(48)))
			p.LTrim(ctx, key, 0, 99)
			_, err := p.Exec(ctx)
			return err
		}},
		{4, func(ctx context.Context, c *redis.Client, r *rand.Rand) error { // LRANGE 0 9 / RPOP
			if r.Intn(5) == 0 {
				err := c.RPop(ctx, k("l:", *colKeys, r)).Err()
				if err == redis.Nil {
					return nil
				}
				return err
			}
			return c.LRange(ctx, k("l:", *colKeys, r), 0, 9).Err()
		}},
		{6, func(ctx context.Context, c *redis.Client, r *rand.Rand) error { // SADD / SREM
			key := k("st:", *colKeys, r)
			if r.Intn(4) == 0 {
				return c.SRem(ctx, key, "m"+strconv.Itoa(r.Intn(200))).Err()
			}
			return c.SAdd(ctx, key, "m"+strconv.Itoa(r.Intn(200))).Err()
		}},
		{3, func(ctx context.Context, c *redis.Client, r *rand.Rand) error { // SISMEMBER / SCARD
			if r.Intn(2) == 0 {
				return c.SCard(ctx, k("st:", *colKeys, r)).Err()
			}
			return c.SIsMember(ctx, k("st:", *colKeys, r), "m"+strconv.Itoa(r.Intn(200))).Err()
		}},
		{6, func(ctx context.Context, c *redis.Client, r *rand.Rand) error { // ZADD / ZINCRBY
			key := k("z:", *colKeys, r)
			m := "m" + strconv.Itoa(r.Intn(200))
			if r.Intn(3) == 0 {
				return c.ZIncrBy(ctx, key, float64(r.Intn(10)), m).Err()
			}
			return c.ZAdd(ctx, key, redis.Z{Score: float64(r.Intn(100000)), Member: m}).Err()
		}},
		{3, func(ctx context.Context, c *redis.Client, r *rand.Rand) error { // ZRANGEBYSCORE LIMIT / ZREVRANGE
			key := k("z:", *colKeys, r)
			if r.Intn(2) == 0 {
				return c.ZRevRangeWithScores(ctx, key, 0, 9).Err()
			}
			lo := r.Intn(90000)
			return c.ZRangeByScore(ctx, key, &redis.ZRangeBy{Min: strconv.Itoa(lo), Max: strconv.Itoa(lo + 10000), Count: 10}).Err()
		}},
		{4, func(ctx context.Context, c *redis.Client, r *rand.Rand) error { // INCR counters
			return c.IncrBy(ctx, k("c:", 10000, r), int64(1+r.Intn(5))).Err()
		}},
		{2, func(ctx context.Context, c *redis.Client, r *rand.Rand) error { // EXPIRE on a collection
			kinds := []string{"h:", "l:", "st:", "z:"}
			return c.Expire(ctx, k(kinds[r.Intn(4)], *colKeys, r), time.Duration(30+r.Intn(570))*time.Second).Err()
		}},
		{1, func(ctx context.Context, c *redis.Client, r *rand.Rand) error { // DEL
			kinds := []string{"s:", "h:", "l:", "st:", "z:"}
			kk := kinds[r.Intn(5)]
			n := *colKeys
			if kk == "s:" {
				n = *strKeys
			}
			return c.Del(ctx, k(kk, n, r)).Err()
		}},
	}
	var fs []op
	var ws []int
	for _, x := range list {
		fs = append(fs, x.f)
		ws = append(ws, x.w)
	}
	return fs, ws
}

func main() {
	flag.Parse()
	bigBuf = []byte(val(rand.New(rand.NewSource(7)), 100_000))
	ctx := context.Background()
	newClient := func() *redis.Client {
		return redis.NewClient(&redis.Options{Addr: *addr, PoolSize: 1, MinIdleConns: 1, ReadTimeout: 5 * time.Second,
			WriteTimeout: 5 * time.Second, MaxRetries: -1, Protocol: 2, DisableIdentity: true})
	}
	if *prefill {
		var wg sync.WaitGroup
		per := *strKeys / *conns
		for g := 0; g < *conns; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				c := newClient()
				r := rand.New(rand.NewSource(int64(g)))
				for i := g * per; i < (g+1)*per; i += 100 {
					p := c.Pipeline()
					for j := i; j < i+100 && j < (g+1)*per; j++ {
						p.Set(ctx, "s:"+strconv.Itoa(j), val(r, 64+r.Intn(448)), 0)
					}
					if _, err := p.Exec(ctx); err != nil {
						fmt.Fprintln(os.Stderr, "prefill:", err)
					}
				}
			}(g)
		}
		wg.Wait()
		return
	}
	fs, ws := ops()
	total := 0
	for _, w := range ws {
		total += w
	}
	pick := make([]int, 0, total)
	for i, w := range ws {
		for j := 0; j < w; j++ {
			pick = append(pick, i)
		}
	}

	var mu sync.Mutex
	hist := hdrhistogram.New(1, 60_000_000, 3)
	var nops, nerr atomic.Int64
	opMax := make([]atomic.Int64, len(fs))
	opSlow := make([]atomic.Int64, len(fs))
	stop := make(chan struct{})
	var wg sync.WaitGroup
	perConnInterval := time.Duration(0)
	if *rate > 0 {
		perConnInterval = time.Duration(float64(time.Second) * float64(*conns) / float64(*rate))
	}
	for g := 0; g < *conns; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			c := newClient()
			r := rand.New(rand.NewSource(*seed*1000 + int64(g)))
			local := hdrhistogram.New(1, 60_000_000, 3)
			lastFlush := time.Now()
			next := time.Now().Add(time.Duration(r.Int63n(int64(perConnInterval) + 1)))
			for {
				select {
				case <-stop:
					mu.Lock()
					hist.Merge(local)
					mu.Unlock()
					return
				default:
				}
				var start time.Time
				if perConnInterval > 0 {
					if d := time.Until(next); d > 0 {
						time.Sleep(d)
					}
					// latency from the scheduled start: a stall shows up in the
					// latency of the operations queued behind it (no coordinated omission)
					start = next
					next = next.Add(perConnInterval)
				} else {
					start = time.Now()
				}
				oi := pick[r.Intn(len(pick))]
				svc := time.Now()
				err := fs[oi](ctx, c, r)
				us := time.Since(start).Microseconds()
				if sv := time.Since(svc).Microseconds(); true {
					for {
						m := opMax[oi].Load()
						if sv <= m || opMax[oi].CompareAndSwap(m, sv) {
							break
						}
					}
					if sv > 10000 {
						opSlow[oi].Add(1)
					}
				}
				if us < 1 {
					us = 1
				}
				_ = local.RecordValue(us)
				nops.Add(1)
				if err != nil {
					nerr.Add(1)
				}
				if time.Since(lastFlush) > 500*time.Millisecond {
					mu.Lock()
					hist.Merge(local)
					mu.Unlock()
					local.Reset()
					lastFlush = time.Now()
				}
			}
		}(g)
	}
	fmt.Println("t_s,ops,ops_per_s,errors,p50_us,p99_us,p999_us,max_us")
	t0 := time.Now()
	tick := time.NewTicker(*interval)
	var lastOps, lastErr int64
	for range tick.C {
		el := time.Since(t0)
		mu.Lock()
		h := hist
		hist = hdrhistogram.New(1, 60_000_000, 3)
		mu.Unlock()
		o, e := nops.Load(), nerr.Load()
		fmt.Printf("%.0f,%d,%.0f,%d,%d,%d,%d,%d\n", el.Seconds(), o-lastOps, float64(o-lastOps)/interval.Seconds(), e-lastErr,
			h.ValueAtQuantile(50), h.ValueAtQuantile(99), h.ValueAtQuantile(99.9), h.Max())
		lastOps, lastErr = o, e
		if *opStats {
			fmt.Fprintf(os.Stderr, "%.0f", el.Seconds())
			for i := range opMax {
				fmt.Fprintf(os.Stderr, " op%d:max=%dus,slow=%d", i, opMax[i].Swap(0), opSlow[i].Swap(0))
			}
			fmt.Fprintln(os.Stderr)
		}
		if el >= *duration {
			break
		}
	}
	close(stop)
	wg.Wait()
}
