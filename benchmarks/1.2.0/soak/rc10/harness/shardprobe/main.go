// shardprobe: open-loop write probe for one shard of a cluster (rc10 soak, #310).
//
// It SETs keys whose slots lie in -slots (a range no event of the soak moves, so the keys stay on one shard and never
// get MOVED/ASK), through go-redis ClusterClient (so it follows the shard's master across failovers), on -conns
// connections at -rate ops/s in all. Latency counts from each operation's scheduled start, so a stalled connection
// shows in every operation queued behind it.
//
// Output:
//   - stdout, one CSV line per -interval: unix_ts,t_s,ops,errors,p50_us,p99_us,p999_us,max_us,stalled_conns
//     (stalled_conns: connections whose worst operation of the interval took over 100 ms, or that have been waiting
//     over 100 ms on one at the tick);
//   - -raw <file>: one line per operation, "<scheduled start, unix us> <latency us> <conn> <0|1 error>", so that any
//     window (a reshard) can be cut exactly afterwards (shardprobe_ana.py).
//
//	shardprobe -addr h1:7001,h2:7001 -slots 2990-3010 -label a -conns 8 -rate 500 -duration 130m -raw a.raw > a.csv
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/redis/go-redis/v9"
)

var (
	addr     = flag.String("addr", "127.0.0.1:7001", "comma-separated seed nodes")
	slots    = flag.String("slots", "2990-3010", "slot range lo-hi of the keys")
	label    = flag.String("label", "p", "key prefix")
	conns    = flag.Int("conns", 8, "connections (one goroutine and one ClusterClient each)")
	rate     = flag.Int("rate", 500, "total ops/s")
	duration = flag.Duration("duration", time.Minute, "run time")
	interval = flag.Duration("interval", time.Second, "CSV interval")
	rawPath  = flag.String("raw", "", "file for one line per operation")
	stopFile = flag.String("stop-file", "", "stop early when this file exists")
)

// CRC16/XMODEM, as Redis Cluster's key hash slot.
func crc16(b []byte) uint16 {
	var crc uint16
	for _, c := range b {
		crc ^= uint16(c) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

func slotOf(k string) int {
	if i := strings.IndexByte(k, '{'); i >= 0 {
		if j := strings.IndexByte(k[i+1:], '}'); j > 0 {
			k = k[i+1 : i+1+j]
		}
	}
	return int(crc16([]byte(k)) % 16384)
}

type rec struct {
	sched int64
	lat   int64
	conn  int
	err   bool
}

func main() {
	flag.Parse()
	lo, hi := 0, 0
	if _, err := fmt.Sscanf(*slots, "%d-%d", &lo, &hi); err != nil {
		panic(err)
	}
	// 64 keys per connection, all in [lo, hi]
	var keys []string
	for i := 0; len(keys) < 64**conns; i++ {
		k := "pr:" + *label + ":" + strconv.Itoa(i)
		if s := slotOf(k); s >= lo && s <= hi {
			keys = append(keys, k)
		}
	}
	val := strings.Repeat("v", 64)
	ctx := context.Background()
	var raw chan rec
	var rawDone sync.WaitGroup
	if *rawPath != "" {
		f, err := os.Create(*rawPath)
		if err != nil {
			panic(err)
		}
		raw = make(chan rec, 1<<16)
		rawDone.Add(1)
		go func() {
			defer rawDone.Done()
			w := bufio.NewWriterSize(f, 1<<20)
			for r := range raw {
				e := 0
				if r.err {
					e = 1
				}
				fmt.Fprintf(w, "%d %d %d %d\n", r.sched, r.lat, r.conn, e)
			}
			w.Flush()
			f.Close()
		}()
	}
	var mu sync.Mutex
	hist := hdrhistogram.New(1, 120_000_000, 3)
	var nops, nerr atomic.Int64
	connMax := make([]atomic.Int64, *conns)
	inflight := make([]atomic.Int64, *conns) // scheduled start (unix us) of the operation a connection is waiting on, or 0
	stop := make(chan struct{})
	var wg sync.WaitGroup
	period := time.Duration(float64(time.Second) * float64(*conns) / float64(*rate))
	t0 := time.Now()
	for g := 0; g < *conns; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(*addr, ","), PoolSize: 1, MinIdleConns: 1,
				ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, MaxRetries: -1, MaxRedirects: 8, Protocol: 2,
				DisableIdentity: true})
			mine := keys[g*64 : (g+1)*64]
			local := hdrhistogram.New(1, 120_000_000, 3)
			lastFlush := time.Now()
			next := t0.Add(period * time.Duration(g) / time.Duration(*conns))
			for i := 0; ; i++ {
				select {
				case <-stop:
					mu.Lock()
					hist.Merge(local)
					mu.Unlock()
					return
				default:
				}
				if d := time.Until(next); d > 0 {
					time.Sleep(d)
				}
				start := next
				next = next.Add(period)
				inflight[g].Store(start.UnixMicro())
				err := c.Set(ctx, mine[i%64], val, 0).Err()
				inflight[g].Store(0)
				us := time.Since(start).Microseconds()
				if us < 1 {
					us = 1
				}
				_ = local.RecordValue(us)
				for {
					m := connMax[g].Load()
					if us <= m || connMax[g].CompareAndSwap(m, us) {
						break
					}
				}
				nops.Add(1)
				if err != nil {
					nerr.Add(1)
				}
				if raw != nil {
					raw <- rec{start.UnixMicro(), us, g, err != nil}
				}
				if time.Since(lastFlush) > 200*time.Millisecond {
					mu.Lock()
					hist.Merge(local)
					mu.Unlock()
					local.Reset()
					lastFlush = time.Now()
				}
			}
		}(g)
	}
	fmt.Println("unix_ts,t_s,ops,errors,p50_us,p99_us,p999_us,max_us,stalled_conns")
	tick := time.NewTicker(*interval)
	var lastOps, lastErr int64
	for range tick.C {
		el := time.Since(t0)
		mu.Lock()
		h := hist
		hist = hdrhistogram.New(1, 120_000_000, 3)
		mu.Unlock()
		stalled := 0
		nowUs := time.Now().UnixMicro()
		for i := range connMax {
			w := inflight[i].Load()
			if connMax[i].Swap(0) > 100_000 || (w != 0 && nowUs-w > 100_000) {
				stalled++
			}
		}
		o, e := nops.Load(), nerr.Load()
		fmt.Printf("%.3f,%.0f,%d,%d,%d,%d,%d,%d,%d\n", float64(time.Now().UnixNano())/1e9, el.Seconds(), o-lastOps, e-lastErr,
			h.ValueAtQuantile(50), h.ValueAtQuantile(99), h.ValueAtQuantile(99.9), h.Max(), stalled)
		lastOps, lastErr = o, e
		if el >= *duration {
			break
		}
		if *stopFile != "" {
			if _, err := os.Stat(*stopFile); err == nil {
				break
			}
		}
	}
	close(stop)
	wg.Wait()
	if raw != nil {
		close(raw)
		rawDone.Wait()
	}
}
