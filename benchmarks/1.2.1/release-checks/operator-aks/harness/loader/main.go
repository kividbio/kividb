// e2e-loader v6: writes <prefix>:<n> = n, records every acknowledged write in a
// bitset, and on /verify reads them all back.
//
// Env: ADDR, MODE=single|cluster, RATE (total SET/s, 800), PASSWORD (optional;
// AUTH default <pw>), PREFIX (e2e). OPENLOOP=1 issues RATE SET/s regardless of
// latency (MAXINFLIGHT outstanding, 4000); otherwise WORKERS (4) closed loops.
// OPTIMEOUT (ms, 0 = none) is a deadline per SET.
// HTTP: /stats /stop /start /verify /timeline /password?p=... /reset-errors
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type bitset struct {
	mu   sync.Mutex
	w    []uint64
	n    int64
	maxN int64
}

func (b *bitset) set(i int64) {
	b.mu.Lock()
	for int64(len(b.w))*64 <= i {
		b.w = append(b.w, make([]uint64, 1<<14)...)
	}
	if b.w[i/64]&(1<<(i%64)) == 0 {
		b.w[i/64] |= 1 << (i % 64)
		b.n++
	}
	if i > b.maxN {
		b.maxN = i
	}
	b.mu.Unlock()
}

func (b *bitset) each(f func(int64)) {
	b.mu.Lock()
	w := append([]uint64(nil), b.w...)
	b.mu.Unlock()
	for wi, x := range w {
		for x != 0 {
			for j := 0; j < 64; j++ {
				if x&(1<<j) != 0 {
					f(int64(wi)*64 + int64(j))
				}
			}
			x = 0
		}
	}
}

var (
	pwMu     sync.RWMutex
	password = os.Getenv("PASSWORD")
)

func creds() (string, string) {
	pwMu.RLock()
	defer pwMu.RUnlock()
	if password == "" {
		return "", ""
	}
	return "default", password
}

type client interface {
	Set(ctx context.Context, key string, value interface{}, exp time.Duration) *redis.StatusCmd
	Get(ctx context.Context, key string) *redis.StringCmd
}

func main() {
	addr := os.Getenv("ADDR")
	mode := os.Getenv("MODE")
	prefix := os.Getenv("PREFIX")
	if prefix == "" {
		prefix = "e2e"
	}
	workers, _ := strconv.Atoi(os.Getenv("WORKERS"))
	if workers == 0 {
		workers = 4
	}
	rate, _ := strconv.Atoi(os.Getenv("RATE"))
	if rate == 0 {
		rate = 800
	}
	openLoop := os.Getenv("OPENLOOP") == "1"
	maxInflight, _ := strconv.Atoi(os.Getenv("MAXINFLIGHT"))
	if maxInflight == 0 {
		maxInflight = 4000
	}
	// OPTIMEOUT (ms): a deadline for each SET, retries and redirects included,
	// like an application's request timeout. A SET past it counts as failed.
	otms, _ := strconv.Atoi(os.Getenv("OPTIMEOUT"))
	opTimeout := time.Duration(otms) * time.Millisecond
	poolSize, poolTimeout := workers+2, 4*time.Second
	if openLoop {
		poolSize, poolTimeout = 64, 30*time.Second
	}
	var rdb client
	var dbsize func() int64
	var cc *redis.ClusterClient
	if mode == "cluster" {
		cc = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: []string{addr}, MaxRedirects: 16, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second,
			DialTimeout: 3 * time.Second, MaxRetries: 0, CredentialsProvider: creds,
			PoolSize: poolSize, PoolTimeout: poolTimeout,
		})
		rdb = cc
		dbsize = func() int64 {
			var t int64
			_ = cc.ForEachMaster(context.Background(), func(ctx context.Context, c *redis.Client) error {
				n, err := c.DBSize(ctx).Result()
				if err == nil {
					atomic.AddInt64(&t, n)
				}
				return nil
			})
			return t
		}
	} else {
		c := redis.NewClient(&redis.Options{
			Addr: addr, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second,
			DialTimeout: 3 * time.Second, MaxRetries: 0, CredentialsProvider: creds,
			PoolSize: poolSize, PoolTimeout: poolTimeout, ConnMaxLifetime: 30 * time.Second,
		})
		rdb = c
		dbsize = func() int64 { n, _ := c.DBSize(context.Background()).Result(); return n }
	}

	var (
		acked   = &bitset{}
		next    atomic.Int64
		errs    atomic.Int64
		mu      sync.Mutex
		last    []string
		slow    []string
		bySec   = map[string]int64{}
		ackSec  = map[int64]int64{} // unix second -> first n started in that second
		tlMu    sync.Mutex
		tl      = map[int64]*[4]int64{} // unix second -> ok, err, max ms, slow
		skipped atomic.Int64
		running atomic.Bool
		cancel  context.CancelFunc
		wg      sync.WaitGroup
	)
	// op issues SET <prefix>:n n and records the outcome.
	op := func(ctx context.Context, n int64) {
		now := time.Now()
		if n%200 == 0 {
			mu.Lock()
			if _, ok := ackSec[now.Unix()]; !ok {
				ackSec[now.Unix()] = n
			}
			mu.Unlock()
		}
		octx := ctx
		if opTimeout > 0 {
			var oc context.CancelFunc
			octx, oc = context.WithTimeout(ctx, opTimeout)
			defer oc()
		}
		err := rdb.Set(octx, fmt.Sprintf("%s:%d", prefix, n), n, 0).Err()
		if ctx.Err() != nil {
			if err == nil {
				acked.set(n)
			}
			return
		}
		d := time.Since(now)
		sec := time.Now().Unix()
		tlMu.Lock()
		c := tl[sec]
		if c == nil {
			c = &[4]int64{}
			tl[sec] = c
		}
		if err == nil {
			c[0]++
		} else {
			c[1]++
		}
		if ms := d.Milliseconds(); ms > c[2] {
			c[2] = ms
		}
		if d > 150*time.Millisecond {
			c[3]++
		}
		tlMu.Unlock()
		if d > 150*time.Millisecond {
			mu.Lock()
			slow = append(slow, fmt.Sprintf("%s %dms ok=%v", now.UTC().Format("15:04:05.000"), d.Milliseconds(), err == nil))
			if len(slow) > 400 {
				slow = slow[len(slow)-400:]
			}
			mu.Unlock()
		}
		if err == nil {
			acked.set(n)
			return
		}
		errs.Add(1)
		mu.Lock()
		bySec[now.UTC().Format("15:04:05")]++
		last = append(last, now.UTC().Format("15:04:05.000 ")+err.Error())
		if len(last) > 30 {
			last = last[len(last)-30:]
		}
		mu.Unlock()
	}
	start := func() {
		ctx, c := context.WithCancel(context.Background())
		cancel = c
		running.Store(true)
		if openLoop {
			// Open loop: RATE SETs a second whatever the latency, at most
			// MAXINFLIGHT outstanding (a SET that finds no slot is skipped and
			// counted in "skipped"). A stalled shard then stalls only its own
			// keys; the other shards' writes go on.
			sem := make(chan struct{}, maxInflight)
			wg.Add(1)
			go func() {
				defer wg.Done()
				tick := time.NewTicker(10 * time.Millisecond)
				defer tick.Stop()
				t0 := time.Now()
				var sent int64
				for {
					select {
					case <-ctx.Done():
						return
					case <-tick.C:
					}
					due := int64(time.Since(t0).Seconds() * float64(rate))
					for ; sent < due; sent++ {
						select {
						case sem <- struct{}{}:
							n := next.Add(1)
							wg.Add(1)
							go func() {
								defer wg.Done()
								defer func() { <-sem }()
								op(ctx, n)
							}()
						default:
							skipped.Add(1)
						}
					}
				}
			}()
			return
		}
		perWorker := time.Duration(float64(time.Second) * float64(workers) / float64(rate))
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				tick := time.NewTicker(perWorker)
				defer tick.Stop()
				for ctx.Err() == nil {
					select {
					case <-ctx.Done():
						return
					case <-tick.C:
					}
					op(ctx, next.Add(1))
				}
			}()
		}
	}
	start()

	// /timeline?since=<unix>: one row per second (completion time):
	// [unix, ok, errors, max latency ms, ops over 150 ms]
	http.HandleFunc("/timeline", func(w http.ResponseWriter, r *http.Request) {
		since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
		tlMu.Lock()
		rows := make([][5]int64, 0, len(tl))
		for s, c := range tl {
			if s >= since {
				rows = append(rows, [5]int64{s, c[0], c[1], c[2], c[3]})
			}
		}
		tlMu.Unlock()
		sort.Slice(rows, func(i, j int) bool { return rows[i][0] < rows[j][0] })
		_ = json.NewEncoder(w).Encode(rows)
	})
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		eb := map[string]int64{}
		for k, v := range bySec {
			eb[k] = v
		}
		l := append([]string(nil), last...)
		sl := append([]string(nil), slow...)
		mu.Unlock()
		acked.mu.Lock()
		a, mx := acked.n, acked.maxN
		acked.mu.Unlock()
		keys := make([]string, 0, len(eb))
		for k := range eb {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		windows := []string{}
		// collapse error seconds into windows
		var ws, we string
		var wc int64
		for _, k := range keys {
			t, _ := time.Parse("15:04:05", k)
			if we != "" {
				pt, _ := time.Parse("15:04:05", we)
				if t.Sub(pt) <= 2*time.Second {
					we = k
					wc += eb[k]
					continue
				}
				windows = append(windows, fmt.Sprintf("%s-%s:%d", ws, we, wc))
			}
			ws, we, wc = k, k, eb[k]
		}
		if we != "" {
			windows = append(windows, fmt.Sprintf("%s-%s:%d", ws, we, wc))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"acked": a, "issued": next.Load(), "maxAcked": mx, "errors": errs.Load(), "skipped": skipped.Load(), "errorWindows": windows, "lastErrors": l, "slow": sl, "running": running.Load()})
	})
	http.HandleFunc("/stop", func(w http.ResponseWriter, r *http.Request) {
		if running.Swap(false) {
			cancel()
			wg.Wait()
		}
		fmt.Fprintln(w, "stopped")
	})
	http.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		if !running.Load() {
			start()
		}
		fmt.Fprintln(w, "started")
	})
	http.HandleFunc("/reset-errors", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		bySec = map[string]int64{}
		last = nil
		slow = nil
		mu.Unlock()
		errs.Store(0)
		fmt.Fprintln(w, "ok")
	})
	http.HandleFunc("/password", func(w http.ResponseWriter, r *http.Request) {
		pwMu.Lock()
		password = r.URL.Query().Get("p")
		pwMu.Unlock()
		fmt.Fprintln(w, "ok")
	})
	// /verify reads every acknowledged key back (stop writes first).
	http.HandleFunc("/verify", func(w http.ResponseWriter, r *http.Request) {
		var missing, wrong, failed atomic.Int64
		var exMu sync.Mutex
		var missList []int64
		ex := map[string]string{}
		ch := make(chan int64, 1024)
		var vg sync.WaitGroup
		for i := 0; i < 32; i++ {
			vg.Add(1)
			go func() {
				defer vg.Done()
				for n := range ch {
					var v string
					var err error
					for try := 0; try < 3; try++ {
						v, err = rdb.Get(context.Background(), fmt.Sprintf("%s:%d", prefix, n)).Result()
						if err == nil || err == redis.Nil {
							break
						}
						time.Sleep(200 * time.Millisecond)
					}
					switch {
					case err == redis.Nil:
						missing.Add(1)
						exMu.Lock()
						missList = append(missList, n)
						exMu.Unlock()
					case err != nil:
						failed.Add(1)
						exMu.Lock()
						if len(ex) < 10 {
							ex[fmt.Sprint(n)] = err.Error()
						}
						exMu.Unlock()
					case v != strconv.FormatInt(n, 10):
						wrong.Add(1)
					}
				}
			}()
		}
		var checked int64
		acked.each(func(n int64) { checked++; ch <- n })
		close(ch)
		vg.Wait()
		sort.Slice(missList, func(i, j int) bool { return missList[i] < missList[j] })
		// map missing n to approximate issue time
		mu.Lock()
		secs := make([]int64, 0, len(ackSec))
		for s := range ackSec {
			secs = append(secs, s)
		}
		sort.Slice(secs, func(i, j int) bool { return secs[i] < secs[j] })
		when := func(n int64) string {
			best := int64(0)
			for _, s := range secs {
				if ackSec[s] <= n {
					best = s
				} else {
					break
				}
			}
			if best == 0 {
				return "?"
			}
			return time.Unix(best, 0).UTC().Format("15:04:05")
		}
		missWhen := map[string]int{}
		for _, n := range missList {
			missWhen[when(n)]++
		}
		mu.Unlock()
		ml := missList
		if len(ml) > 40 {
			ml = ml[:40]
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"checked": checked, "missing": missing.Load(), "wrong": wrong.Load(), "readErrors": failed.Load(), "missingKeys": ml, "missingBySecond": missWhen, "examples": ex, "dbsize": dbsize()})
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}
