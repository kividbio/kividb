// go-redis runner: run.py's contract (host port protocol cases.json).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

type testCase struct {
	Name string   `json:"name"`
	Cmd  []string `json:"cmd"`
}

// norm turns a reply into JSON-able values: maps as [key, value] pairs, sets sorted.
func norm(v any) any {
	switch x := v.(type) {
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = norm(e)
		}
		return out
	case map[any]any:
		keys := make([]string, 0, len(x))
		byKey := map[string]any{}
		for k, e := range x {
			ks := fmt.Sprint(k)
			keys = append(keys, ks)
			byKey[ks] = e
		}
		// Go maps have no order: pairs sorted by key.
		sort.Strings(keys)
		out := make([]any, len(keys))
		for i, k := range keys {
			out[i] = []any{k, norm(byKey[k])}
		}
		return out
	case map[string]any:
		m := map[any]any{}
		for k, e := range x {
			m[k] = e
		}
		return norm(m)
	case float64:
		if math.IsInf(x, 0) || math.IsNaN(x) {
			return strconv.FormatFloat(x, 'g', -1, 64)
		}
		return x
	}
	return v
}

func main() {
	port, _ := strconv.Atoi(os.Args[2])
	protocol, _ := strconv.Atoi(os.Args[3])
	raw, err := os.ReadFile(os.Args[4])
	if err != nil {
		panic(err)
	}
	var cases []testCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		panic(err)
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:       fmt.Sprintf("%s:%d", os.Args[1], port),
		Protocol:   protocol,
		PoolSize:   1,
		MaxRetries: -1,
	})
	conn := rdb.Conn()
	if err := conn.Ping(ctx).Err(); err != nil {
		panic(err)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(map[string]any{"client": "go-redis", "version": redis.Version()})
	for _, c := range cases {
		args := make([]any, len(c.Cmd))
		for i, a := range c.Cmd {
			args[i] = a
		}
		v, err := conn.Do(ctx, args...).Result()
		var rerr redis.Error
		switch {
		case errors.Is(err, redis.Nil):
			_ = enc.Encode(map[string]any{"case": c.Name, "reply": nil})
		case errors.As(err, &rerr):
			_ = enc.Encode(map[string]any{"case": c.Name, "error": strings.SplitN(rerr.Error(), " ", 2)[0]})
		case err != nil:
			panic(err)
		default:
			_ = enc.Encode(map[string]any{"case": c.Name, "reply": norm(v)})
		}
	}
}
