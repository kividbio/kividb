# LangGraph

`langgraph-checkpoint-redis` works on KiviDB: the checkpointer (`RedisSaver`, `ShallowRedisSaver`,
`AsyncRedisSaver`) and the long-term store (`RedisStore`, with or without a vector index). Point its
Redis URL at KiviDB (port `6380` by default) and nothing else changes.

**Checked:** 23 of 23 checks pass on KiviDB, the same 23 that pass on Redis 8, with
`langgraph-checkpoint-redis` `0.6.0`. See the [report](../../benchmarks/langgraph-compat/REPORT.md) for
each check, what it does not cover, and how to run it yourself.

## Example

```bash
docker run -d -p 6380:6380 kividb/kividb
pip install langgraph langgraph-checkpoint-redis
```

```python
import operator
from typing import Annotated, TypedDict

from langgraph.checkpoint.redis import RedisSaver
from langgraph.graph import END, START, StateGraph


class State(TypedDict):
    n: int
    log: Annotated[list, operator.add]


def inc(state):
    return {"n": state["n"] + 1, "log": [f"inc -> {state['n'] + 1}"]}


builder = StateGraph(State)
builder.add_node("inc", inc)
builder.add_edge(START, "inc")
builder.add_edge("inc", END)

with RedisSaver.from_conn_string("redis://localhost:6380") as saver:  # KiviDB, not Redis's 6379
    saver.setup()  # creates its search indexes
    app = builder.compile(checkpointer=saver)
    config = {"configurable": {"thread_id": "demo"}}

    app.invoke({"n": 1, "log": []}, config)
    print(app.invoke({"n": 10, "log": []}, config))  # the same thread: its state carries on
    print(len(list(app.get_state_history(config))), "checkpoints")
```

It prints (the same on KiviDB and on Redis 8):

```
{'n': 11, 'log': ['inc -> 2', 'inc -> 11']}
6 checkpoints
```

Each thread's state is kept, and the second call continues the first thread's `log`. The history lists
every checkpoint, newest first.

The store works the same way: `RedisStore.from_conn_string("redis://localhost:6380")`, then `store.setup()`.
Pass an `index` with an embedding model to search it by meaning.

## Known limits

- `RedisSaver.copy_thread` raises `NotImplementedError` in `langgraph-checkpoint-redis` itself, on
  Redis 8 as well.
- Scale is not tested: the harness uses a few threads and a handful of checkpoints, not many threads,
  long histories or large states.
- LlamaIndex and Mem0 are not covered yet. See [#33](https://github.com/kividbio/kividb/issues/33).
