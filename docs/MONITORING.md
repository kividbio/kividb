# Monitoring with Prometheus and Grafana

KiviDB has no metrics endpoint of its own. It answers `INFO` as Redis does,
so it is monitored the way Redis is: with
[redis_exporter](https://github.com/oliver006/redis_exporter), which reads
`INFO` and serves Prometheus metrics. The KiviDB operator runs the same
exporter as a sidecar.

## Running the exporter

Run one exporter for each node, next to it:

```bash
docker run -d --network host -e REDIS_ADDR=redis://127.0.0.1:6380 \
  -e REDIS_PASSWORD=change-me oliver006/redis_exporter:v1.66.0
```

It serves on port 9121. Prometheus scrapes it:

```yaml
scrape_configs:
  - job_name: kividb
    static_configs:
      - targets: ["10.0.0.1:9121", "10.0.0.2:9121"]
```

On Kubernetes with the KiviDB operator, set `spec.monitoring.enabled: true`
on the `KividbCluster`: the operator adds the exporter to each pod, and its
chart's `ServiceMonitor` (`metrics.serviceMonitor.enabled`) has Prometheus
scrape it.

## The dashboard

[`examples/monitoring/grafana/dashboards/kividb.json`](../examples/monitoring/grafana/dashboards/kividb.json)
is a Grafana dashboard for these metrics. Import it (Dashboards → New →
Import) and pick your Prometheus data source. It has:

- **Overview**: up, uptime, role, clients, keys, memory and hit ratio, per
  node;
- **Commands**: commands per second, in all and by command, mean time per
  call, p99 latency, errors by type, failed and rejected calls;
- **Memory and keys**: used memory and RSS against `maxmemory`, keys by
  database, expired and evicted keys;
- **Clients and network**: connected and blocked clients, connections
  accepted and rejected, bytes in and out;
- **Persistence and replication**: time since the last save and its
  result, the AOF, each replica's link, connected replicas and their lag.

To try it, [`examples/monitoring`](../examples/monitoring) runs a master and
a replica, an exporter for each, Prometheus and Grafana:

```bash
cd examples/monitoring
docker compose --profile load up -d    # Grafana on http://localhost:3000
docker compose --profile load down     # when done
```

The `load` profile runs `redis-benchmark` against the master, so the graphs
move.

## What the exporter reports

Every release, the compat workflow checks what redis_exporter v1.66.0
reports for KiviDB against what it reports for Redis 8.10.3
([`benchmarks/redis-exporter-compat`](../benchmarks/redis-exporter-compat)).
Under the same workload it checks that the exporter scrapes without errors,
that 14 metrics read what the workload did (keys per database, command and
error counts, hits and misses, blocked clients, the role, latency
percentiles), and that each metric Redis has is there.

51 of the exporter's metrics are missing for KiviDB, because its `INFO` has
no field for them yet. They're listed in
[`known-gaps.txt`](../benchmarks/redis-exporter-compat/known-gaps.txt), and
include pub/sub channel and pattern counts, watched keys, ACL denials, the
allocator's fragmentation figures, and Lua memory. The dashboard uses none
of them.

The exporter knows only Redis's `INFO` fields, so it leaves out the ones
KiviDB adds (`kividb_version`, `kividb_variant` and the rest). Read those
with `INFO` directly.

## Alerts worth having

| Alert | Expression |
|---|---|
| A node is down | `redis_up == 0` |
| A replica lost its master | `redis_master_link_up == 0` |
| The last save failed | `redis_rdb_last_bgsave_status == 0` |
| Memory close to `maxmemory` | `redis_memory_used_bytes / redis_memory_max_bytes > 0.9 and redis_memory_max_bytes > 0` |
| Connections refused at `maxclients` | `increase(redis_rejected_connections_total[5m]) > 0` |
