# Stateless external cluster autoscaler

A program that performs one run and exits. A run reads the catalog for every
configured cluster, samples busy time from each hydrated replica, computes a
recommended size, and issues at most one `ALTER CLUSTER ... SET (SIZE)` per
cluster. It keeps no state between runs.

Design: [`doc/developer/design/20261114_stateless_external_autoscaler.md`](../../../../doc/developer/design/20261114_stateless_external_autoscaler.md).
The demand model is the one in
[`20261114_reactive_cluster_autoscaling.md`](../../../../doc/developer/design/20261114_reactive_cluster_autoscaling.md).

## Running

```
bin/cluster-autoscaler autoscaler.yml --dry-run
bin/cluster-autoscaler autoscaler.yml
bin/cluster-autoscaler autoscaler.yml --loop --interval 2min
```

Run it on a schedule, every one to five minutes. `--loop` repeats runs in one
process and carries nothing from one run to the next. Start with `--dry-run`,
which reports every decision and issues no `ALTER`.

Each run prints one JSON record per cluster to stdout. Logs go to stderr.

| Exit code | Meaning |
|---|---|
| 0 | Every cluster was evaluated, whatever was decided. |
| 1 | A cluster could not be evaluated, or an `ALTER` was rejected. |
| 2 | The configuration is invalid. Nothing was evaluated. |

## Configuration

```yaml
connection:
  host: example.materialize.cloud
  port: 6875                                    # default
  user: autoscaler
  password_file: /var/run/secrets/mz-password   # or MZ_PASSWORD
  sslmode: require                              # default
  database: materialize                         # default

defaults:
  cooldown: 1h                # default
  sample_interval: 30s        # default, gap between the two busy-time reads
  replica_read_timeout: 10s   # default, statement timeout for targeted reads

clusters:
  - name: analytics
    min_size: 100cc
    max_size: 800cc
    cooldown: 2h
  - name: serving
    min_size: 200cc
    max_size: 400cc

dry_run: false
```

- The password comes from `password_file` or from `MZ_PASSWORD`. A password in
  the file itself is rejected.
- `cooldown` is the minimum time between size changes and also the window that
  must look quiet before a scale-down. Lag history is retained for one day, so
  a longer cooldown checks lag over the last day only.
- Clusters not listed are never touched. Unmanaged clusters and clusters with
  `SCHEDULE = ON REFRESH` are skipped with a warning.
- The sizes a cluster may take are those between `min_size` and `max_size`
  whose names differ from them only in their numbers, such as `100cc` to
  `800cc`. The catalog does not expose a size family, so this is by name. For
  sizes that do not follow such a pattern, list the ladder:

  ```yaml
  - name: legacy
    min_size: small
    max_size: large
    sizes: [small, medium, large]
  ```
- Sizes are checked at startup against `mz_cluster_replica_sizes`, which lists
  enabled sizes only.

The model parameters are flags and apply to every cluster. See
`bin/cluster-autoscaler --help`. The defaults are those of the design:
`--u-target 0.7`, `--m-target 0.8`, `--min-progress-ratio 0.25`,
`--catch-up 15min`, `--max-lag 60s`.

## Required privileges

Run the autoscaler as a dedicated role. Its `ALTER`s then stand out in
`mz_audit_events`, and a failed attempt is attributed to it for backoff.

```sql
CREATE ROLE autoscaler WITH LOGIN PASSWORD '...';
-- Targeted reads.
GRANT USAGE ON CLUSTER analytics, serving TO autoscaler;
-- ALTER CLUSTER requires ownership.
ALTER CLUSTER analytics OWNER TO autoscaler;
ALTER CLUSTER serving OWNER TO autoscaler;
```

The catalog read runs on `mz_catalog_server` and uses relations that are
readable by every role. A dry run needs only the `USAGE` grants.

This grant set has not been verified against a live environment yet.

## What a record says

```json
{
  "cluster": "analytics",
  "state": "eligible",
  "current_size": "200cc",
  "recommended_size": "400cc",
  "decision": "scale-up",
  "reason": "cpu demand 3.50 cores exceeds 4 cores at 0.7 target",
  "target_size": "400cc",
  "action": "altered",
  "dry_run": false,
  "mode": "sampled",
  "cpu_demand_cores": 3.5,
  "replicas": [
    {"name": "r1", "size": "200cc", "sampled": true, "status": "sampled",
     "timeouts": 0, "busy_cores": 3.5, "cpu_demand_cores": 3.5,
     "max_worker_utilization": 0.97, "min_progress_ratio": 1.0,
     "slowest_dataflow": null, "interval_seconds": 30.0}
  ],
  "window": {"seconds": 7200.0, "peak_cpu_cores": 3.6,
             "peak_cpu_percent": 90.0, "peak_memory_bytes": 12884901888,
             "peak_lag_seconds": 3.0}
}
```

- `state` is the gate the catalog read sorted the cluster into:
  `reconfiguring`, `oom`, `cooling-down`, `hydrating`, `eligible`, `skipped`,
  or `error`.
- `decision` is `scale-up`, `scale-down`, a reason for doing nothing
  (`steady`, `at-min`, `at-max`, `skew-limited`, `backing-off`), the gating
  state when the cluster was not eligible, or `error`.
- `action` is `altered`, `dry-run`, `none`, `superseded` (the cluster was
  reconfigured by someone else during the run, so no `ALTER` was issued), or
  `failed` (the `ALTER` was rejected).
- `mode` is `sampled` when a live sample sized the decision and `fallback`
  when the cluster has introspection disabled or runs no compute objects. A
  fallback decision uses memory, lag, process CPU and out-of-memory kills
  only.
- A replica with `"hydrated": false` was not targeted. A replica with
  `"status": "unresponsive"` hit the statement timeout twice in a row.

## Layout

| File | Contents |
|---|---|
| `decision.py` | Gating and the decision rules. Pure. |
| `sampling.py` | Two introspection reads to a load measurement. Pure. |
| `history.py` | Audit events and metrics rows to window facts. Pure. |
| `catalog.py` | Every SQL statement. |
| `runner.py` | One run: read, gate, sample, decide, act, report. |
| `config.py` | The YAML configuration. |

## Testing

```
bin/pytest misc/python/materialize/cluster_autoscaler
bin/mzcompose --find cluster-autoscaler run default
```
