# Plan: LogQL-to-ClickHouse Loki-Compatible Service

## Overview

A standalone Go service that exposes the Loki HTTP API and translates LogQL queries
into ClickHouse SQL at runtime — against **any user-provided table schema**, with no
requirement to adopt a specific data model.

The existing gigapipe/qryn translator is tightly coupled to a fixed 3-table normalized
schema (`samples_v3`, `time_series`, `time_series_gin`). This new service replaces that
coupling with a configurable or auto-discovered schema layer, so users can point it at
tables they already own.

---

## Goals

- Expose the Loki v1 HTTP API (query_range, query, labels, label values, series, tail)
- Accept LogQL and translate it to ClickHouse SQL
- Work against any flat or semi-structured ClickHouse table
- Support two schema modes: **manual config** and **auto-discovery**
- No dependency on the qryn data model
- Reuse the existing LogQL parser (`reader/logql/logql_parser`) as a library

---

## Non-Goals

- Write path / ingestion (Loki push API)
- Prometheus / Tempo / Pyroscope APIs
- Support for the normalized qryn 3-table schema (that stays in gigapipe)
- Multi-tenancy (out of scope for v1)

---

## Schema Modes

### Mode A — Flat Table (primary target)

User has one table where each row is a log line and labels are regular columns:

```sql
CREATE TABLE app_logs (
    ts          DateTime64(9),
    level       LowCardinality(String),
    host        String,
    service     LowCardinality(String),
    message     String
) ENGINE = MergeTree() ORDER BY ts;
```

LogQL `{level="error", host=~"web.*"} |= "timeout" | level != "debug"`
translates directly to:

```sql
SELECT
    ts                                           AS timestamp_ns,
    message                                      AS body,
    map('level', level, 'host', host,
        'service', service)                      AS labels
FROM app_logs
WHERE ts BETWEEN {from} AND {to}
  AND level = 'error'
  AND match(host, 'web.*')
  AND message LIKE '%timeout%'
  AND level != 'debug'
ORDER BY ts DESC
LIMIT 1000
```

### Mode B — JSON Column (semi-structured)

User has a table with a JSON/String column holding structured log data alongside
fixed timestamp and message columns. Labels are extracted from the JSON at query time:

```sql
CREATE TABLE app_logs (
    ts      DateTime64(9),
    message String,
    attrs   String   -- '{"level":"error","host":"web-01"}'
) ENGINE = MergeTree() ORDER BY ts;
```

Label filters become `JSONExtractString(attrs, 'level') = 'error'`.

---

## Configuration

### File: `config.yaml`

```yaml
clickhouse:
  host: localhost
  port: 9000
  database: mydb
  username: default
  password: ""
  tls: false

server:
  host: 0.0.0.0
  port: 3100
  read_timeout: 30s
  write_timeout: 30s

schema:
  mode: flat              # flat | json_column | otel
  auto_discover: false    # if true, ignores columns below and runs DESCRIBE TABLE

  # Only used when auto_discover: true
  # How often (in seconds) to re-run DESCRIBE TABLE and refresh the column mapping.
  # 0 means discover once at startup and never refresh.
  auto_discover_refresh_seconds: 300   # default: 5 minutes

  table: app_logs

  columns:
    timestamp: ts                    # required: DateTime64 or Int64/UInt64
    timestamp_unit: ns               # ns | us | ms | s  (only relevant for integer columns)
    message: message                 # required: the log body column

    # flat mode: list of columns that become LogQL label dimensions
    labels:
      - level
      - host
      - service

    # json_column mode: single column holding key-value pairs
    # json_column: attrs

    # otel mode: map columns searched for dynamic label keys (in priority order)
    # map_columns:
    #   - LogAttributes
    #   - ResourceAttributes
    #   - ScopeAttributes

query:
  default_limit: 1000
  max_limit: 5000
  default_lookback: 1h       # used for /loki/api/v1/labels and /series
```

### Environment Variable Overrides

All config keys are overridable via env vars using `LOKI_CH_` prefix and `__` as
separator, e.g.:
- `LOKI_CH_CLICKHOUSE__HOST=db.internal`
- `LOKI_CH_SCHEMA__TABLE=my_table`
- `LOKI_CH_SCHEMA__AUTO_DISCOVER=true`
- `LOKI_CH_SCHEMA__AUTO_DISCOVER_REFRESH_SECONDS=60`

---

## Auto-Discovery

When `schema.auto_discover: true`, on startup the service runs:

```sql
DESCRIBE TABLE {database}.{table}
```

and applies the following heuristics to map columns:

| Role | Detection rule |
|---|---|
| **timestamp** | Type is `DateTime64`, `DateTime`, or `Int64`/`UInt64` with name containing `time`, `ts`, `timestamp`, `date` |
| **message** | Type is `String`, not low-cardinality, name contains `message`, `msg`, `body`, `log`, `text`, or is the only large String column |
| **label** | Type is `LowCardinality(String)`, or `String`/`Enum` with name not matching message heuristic, cardinality < threshold |
| **value** | Type is `Float64`, `Float32`, `Int*`, name contains `value`, `val`, `metric`, `count` |
| **map** | Type is `Map(*)` — treated as dynamic label bag (OTel attributes columns) |

### Schema Refresh

When `auto_discover_refresh_seconds > 0`, a background goroutine re-runs discovery on
that interval. This allows the service to pick up schema changes (e.g. a new label
column added to the table, or new keys appearing in a Map column) without a restart.

```
startup
  └─ discoverSchema() → builds SchemaConfig, stores in atomic.Pointer[SchemaConfig]
       └─ if refresh_seconds > 0:
            └─ goroutine: ticker(refresh_seconds)
                 └─ on tick: discoverSchema() → atomic.Store(newSchema)
                      └─ log "schema refreshed: added columns [foo, bar]"
```

Queries always read the schema via `atomic.Load`, so a refresh mid-flight is safe —
the old schema finishes the in-progress query while new queries pick up the updated one.

The current live schema is always visible at `GET /config`.

Discovery result is logged at startup and can be dumped via `GET /config` so users
can verify or copy it into an explicit config.

---

## Project Structure

```
logql-clickhouse/           (new standalone service, separate from gigapipe)
├── cmd/
│   └── server/
│       └── main.go               # flag parsing, config load, wiring, http.ListenAndServe
│
├── internal/
│   ├── config/
│   │   └── config.go             # Config struct, YAML loader, env overrides
│   │
│   ├── schema/
│   │   ├── schema.go             # SchemaConfig, column role types, validation
│   │   └── discovery.go          # DESCRIBE TABLE → SchemaConfig heuristics
│   │
│   ├── clickhouse/
│   │   └── client.go             # clickhouse-go v2 connection pool, Ping, Query
│   │
│   ├── translator/
│   │   ├── translator.go         # entry point: LogQL string + SchemaConfig → SQL string
│   │   ├── stream_select.go      # {label="val"} → WHERE col = 'val'
│   │   ├── line_filter.go        # |= "text", |~ "regex", != "text"
│   │   ├── label_filter.go       # | label op value  (post-parse pipeline filters)
│   │   ├── parser_stage.go       # | json, | logfmt, | pattern  (best-effort column extraction)
│   │   ├── unwrap.go             # | unwrap col  (for metric queries)
│   │   ├── range_agg.go          # count_over_time, rate, sum_over_time, etc.
│   │   ├── vector_agg.go         # sum/min/max/avg/count by(labels)
│   │   └── labels_query.go       # SQL for /labels and /label/{name}/values
│   │
│   ├── loki/
│   │   ├── router.go             # gorilla/mux route registration
│   │   ├── handlers.go           # HTTP handler functions
│   │   ├── params.go             # query param parsing (start, end, step, limit, query)
│   │   └── models.go             # Loki JSON response structs
│   │
│   └── server/
│       └── server.go             # middleware stack, graceful shutdown
│
├── config.yaml                   # example / default config
├── go.mod
└── go.sum
```

---

## API Endpoints

All endpoints follow the [Loki HTTP API spec](https://grafana.com/docs/loki/latest/reference/loki-http-api/).

| Method | Path | Handler | Notes |
|---|---|---|---|
| GET/POST | `/loki/api/v1/query_range` | `QueryRange` | Range log/metric queries |
| GET/POST | `/loki/api/v1/query` | `QueryInstant` | Instant queries |
| GET/POST | `/loki/api/v1/labels` | `Labels` | All label names |
| GET/POST | `/loki/api/v1/label/{name}/values` | `LabelValues` | Values for a label |
| GET/POST | `/loki/api/v1/series` | `Series` | Series matching a selector |
| GET | `/loki/api/v1/tail` | `Tail` | WebSocket streaming (v2) |
| GET | `/ready` | `Ready` | Health check |
| GET | `/config` | `ShowConfig` | Dump resolved schema config |
| GET | `/metrics` | Prometheus metrics | Request counts, latencies |

---

## Translation Pipeline

For each incoming LogQL query the translator works in stages:

```
LogQL string
    │
    ▼
logql_parser.Parse()          (reuse from gigapipe reader/logql/logql_parser)
    │  returns *LogQLScript AST
    ▼
translator.Translate(ast, schema, params)
    │
    ├─ StreamSelectBuilder     {label=val} → WHERE clauses on label columns
    ├─ LineFilterBuilder       |= |~ !~ → WHERE message LIKE / match()
    ├─ LabelFilterBuilder      | label op val → additional WHERE clauses
    ├─ ParserStageBuilder      | json | logfmt → JSONExtract or column alias
    ├─ UnwrapBuilder           | unwrap col → cast to Float64
    ├─ RangeAggBuilder         count_over_time([5m]) → tumbling window GROUP BY
    └─ VectorAggBuilder        sum by (label) → outer GROUP BY
    │
    ▼
SQL string + args
    │
    ▼
clickhouse.Query(sql, args)
    │
    ▼
Result rows → Loki JSON response
```

### Stream Selector Translation Detail

**Flat mode:**
```
{level="error"}         →  WHERE level = 'error'
{host=~"web-.*"}        →  WHERE match(host, 'web-.*')
{host!~"db-.*"}         →  WHERE NOT match(host, 'db-.*')
{level!="debug"}        →  WHERE level != 'debug'
{}  (empty selector)    →  (no WHERE clause added)
```

**JSON column mode:**
```
{level="error"}   →  WHERE JSONExtractString(attrs, 'level') = 'error'
{host=~"web.*"}   →  WHERE match(JSONExtractString(attrs, 'host'), 'web.*')
```

### Range Aggregation Translation

`count_over_time({service="api"}[5m])` with `step=1m`:

```sql
SELECT
    toStartOfInterval(ts, INTERVAL 1 MINUTE) AS t,
    count(*)                                  AS value,
    map('service', service)                   AS labels
FROM app_logs
WHERE ts BETWEEN {from} AND {to}
  AND service = 'api'
GROUP BY t, service
ORDER BY t ASC
```

`rate({service="api"}[5m])` = count_over_time / window_seconds:

```sql
SELECT
    toStartOfInterval(ts, INTERVAL 1 MINUTE) AS t,
    count(*) / 300.0                          AS value,
    map('service', service)                   AS labels
FROM app_logs
WHERE ts BETWEEN {from} AND {to}
  AND service = 'api'
GROUP BY t, service
ORDER BY t ASC
```

### Label / Series Queries

`GET /loki/api/v1/labels` in flat mode:

```sql
-- one query per label column; results merged in Go
SELECT DISTINCT level   FROM app_logs WHERE ts > {lookback_from}
SELECT DISTINCT host    FROM app_logs WHERE ts > {lookback_from}
SELECT DISTINCT service FROM app_logs WHERE ts > {lookback_from}
```

`GET /loki/api/v1/series?match[]={service="api"}`:

```sql
SELECT DISTINCT level, host, service
FROM app_logs
WHERE ts BETWEEN {from} AND {to}
  AND service = 'api'
```

Each distinct row becomes one series object with label map.

---

## Loki Response Format

The service returns standard Loki JSON. Examples:

**Log stream result (`/query_range` with log query):**
```json
{
  "status": "success",
  "data": {
    "resultType": "streams",
    "result": [
      {
        "stream": {"level": "error", "host": "web-01"},
        "values": [
          ["1711490400000000000", "connection timeout after 30s"]
        ]
      }
    ]
  }
}
```

**Matrix result (`/query_range` with metric query):**
```json
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {"service": "api"},
        "values": [[1711490400, "42"]]
      }
    ]
  }
}
```

---

## Key Dependencies (go.mod)

```
github.com/ClickHouse/clickhouse-go/v2    v2.x   -- official ClickHouse driver
github.com/gorilla/mux                    v1.x   -- HTTP router
github.com/gorilla/websocket              v1.x   -- /tail websocket
gopkg.in/yaml.v3                                 -- config parsing
github.com/prometheus/client_golang              -- /metrics endpoint
```

**Reused from gigapipe (imported as a local module path or copied):**
```
github.com/metrico/qryn/v4/reader/logql/logql_parser   -- LogQL parser + AST
```

---

## Implementation Phases

### Phase 1 — Foundation
- [ ] `go mod init` new module `github.com/metrico/logql-clickhouse`
- [ ] `internal/config`: Config struct, YAML loader, env var overrides
- [ ] `internal/clickhouse`: Connection pool using clickhouse-go v2, Ping on startup
- [ ] `internal/schema`: SchemaConfig struct, validation (required fields check)
- [ ] `cmd/server/main.go`: Wire config → schema → db → server, graceful shutdown
- [ ] `GET /ready` and `GET /config` endpoints

### Phase 2 — Schema Discovery
- [ ] `internal/schema/discovery.go`: `DESCRIBE TABLE` query + heuristic column mapping
- [ ] Timestamp unit detection (DateTime64 precision vs Int64 epoch)
- [ ] Log discovered schema at INFO level on startup
- [ ] Discovery result serialized and served by `GET /config`

### Phase 3 — Log Query Translation
- [ ] `internal/translator/stream_select.go`: stream selector → WHERE (flat + json modes)
- [ ] `internal/translator/line_filter.go`: line filters → WHERE LIKE / match()
- [ ] `internal/translator/label_filter.go`: pipeline label filters
- [ ] `internal/translator/translator.go`: wire the above into a full SELECT statement
- [ ] `GET /loki/api/v1/query_range` for log stream queries
- [ ] `GET /loki/api/v1/query` instant log queries

### Phase 4 — Label & Series Queries
- [ ] `internal/translator/labels_query.go`: DISTINCT queries per label column
- [ ] `GET /loki/api/v1/labels`
- [ ] `GET /loki/api/v1/label/{name}/values`
- [ ] `GET /loki/api/v1/series`

### Phase 5 — Metric Queries
- [ ] `internal/translator/range_agg.go`: count_over_time, rate, bytes_over_time, sum_over_time, min_over_time, max_over_time, avg_over_time
- [ ] `internal/translator/unwrap.go`: | unwrap col support
- [ ] `internal/translator/vector_agg.go`: sum/min/max/avg/count by()/without()
- [ ] Matrix result type in response models
- [ ] `GET /loki/api/v1/query_range` metric path

### Phase 6 — Tail / Streaming
- [ ] WebSocket handler for `/loki/api/v1/tail`
- [ ] Polling loop: repeated ClickHouse queries with advancing `from` cursor
- [ ] Configurable poll interval (default 1s)

### Phase 7 — Hardening
- [ ] SQL injection prevention: all user values as query parameters (no string interpolation)
- [ ] Request timeout propagation via `context`
- [ ] Prometheus metrics: request count, error rate, query latency histogram
- [ ] Structured JSON logging (zerolog or slog)
- [ ] Docker image + example docker-compose with ClickHouse
- [ ] Integration test suite with testcontainers-go

---

## SQL Safety

All user-supplied values (label values, line filter strings, regex patterns) **must** be
passed as ClickHouse query parameters, never interpolated into SQL strings directly.

```go
// Safe
db.QueryContext(ctx, "SELECT * FROM t WHERE level = {level:String}",
    clickhouse.Named("level", userValue))

// Never do this
fmt.Sprintf("SELECT * FROM t WHERE level = '%s'", userValue)  // SQL injection
```

Regex patterns used in `match()` are validated against `syntax.Parse()` before being
sent to ClickHouse.

---

## Open Questions / Future Work

- **Parser stage** (`| json`, `| logfmt`): for flat mode this is a no-op since columns
  are already structured. For json_column mode, `| json` would extract sub-keys from the
  JSON column. Full support is a stretch goal.
- **`| pattern` and `| regexp`**: capture group extraction into virtual label keys.
  Feasible with `extract()` in ClickHouse but increases SQL complexity.
- **Caching**: optional query result cache (Redis or in-process LRU) to handle repeated
  dashboard panel requests without hitting ClickHouse.
- **Multiple tables**: routing different label selectors to different tables (e.g.
  `{source="nginx"}` → `nginx_logs`, `{source="app"}` → `app_logs`).
- **Write path**: a future phase could add the Loki push API writing to ClickHouse in
  the flat schema format.
