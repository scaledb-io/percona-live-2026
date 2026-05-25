---
theme: default
title: Streaming MySQL Changes to ClickHouse
info: |
  Percona Live 2026 · USA
  Javier Zon · Founder, ScaleDB
class: text-center
highlighter: shiki
lineNumbers: false
drawings:
  persist: false
transition: fade
mdc: true
colorSchema: light
---

# Streaming MySQL Changes to ClickHouse

## Designing an End-to-End CDC Pipeline

Javier Zon · Founder, ScaleDB

Percona Live 2026 · USA

<!-- One-line hook — "We stream every change from MySQL into a ClickHouse data lake of 80 billion+ events and 35TB. Here's the architecture, and the scars we earned building it." -->

---
layout: default
---

# Analytics Was Killing the Source Database

- BI + dashboards ran against MySQL read replicas — slow, fragile, contended
- Billions of events; cross-domain joins (orders × contacts × billing) timed out
- We needed **fresh** analytics without touching production write paths
- Goal: real-time CDC into a columnar store built for analytics

<!-- Frame the pain everyone in the room feels. Read replicas are a band-aid; analytical queries and OLTP don't share well. -->

---
layout: center
---

# What We're Actually Moving

<div class="grid grid-cols-2 gap-4">
  <div class="stat-card">
    <div class="stat-number">80B+</div>
    <div class="stat-label">Events in ClickHouse</div>
  </div>
  <div class="stat-card">
    <div class="stat-number">35TB</div>
    <div class="stat-label">Analytical data</div>
  </div>
  <div class="stat-card">
    <div class="stat-number">64</div>
    <div class="stat-label">Redpanda partitions (RF=3)</div>
  </div>
  <div class="stat-card">
    <div class="stat-number">~$2.7k</div>
    <div class="stat-label">Full production lake cost/mo</div>
  </div>
</div>

<!-- Establish credibility through scale; the cost stat lands with budget owners — a full real-time lake for the price of a few RDS instances. -->

---
layout: image-right
image: /graphics/g01_pipeline.png
---

# The End-to-End Pipeline

**Loose coupling** — Redpanda buffers so ClickHouse downtime never loses data.

<!-- Walk the path of a single row change end to end in ~30 seconds. Emphasize loose coupling — Redpanda buffers so ClickHouse downtime never loses data. -->

---
layout: default
---

# Four Decisions That Shaped Everything

**01 · ReplacingMergeTree(`_version`)**
Last-writer-wins dedup · `_version = updated_at`

**02 · Soft Deletes**
Deletes become a `_deleted` flag, not row removal

**03 · Read-Only & PII-Safe by Construction**
Sensitive columns never land in analytics tables

**04 · Buffer, Don't Couple**
Redpanda absorbs spikes and outages

<!-- These four show up again in every war story. Plant them now. -->

---
layout: default
---

# Capturing Change Without Re-Reading MySQL

- One Debezium connector **per domain** (orders, contacts, products, …)
- `snapshot.mode: schema_only` → start at current binlog, never resnapshot history
- Redpanda (Kafka API): **3 nodes, RF=3, 64 partitions**, topic per table
- Why Redpanda: no ZooKeeper, simpler ops, NVMe nodes

The connector config is short — six settings do the real work.

<!-- Why Redpanda over Kafka: simpler ops, no ZooKeeper, NVMe nodes. schema_only is the setup for the next big story (we bootstrap history differently). Tease the config: "six settings do the real work" → next slide. -->

---
layout: default
class: zoom-code
---

# Capturing Change Without Re-Reading MySQL
## — the connector config

```json {2-3|4-5|6-9|10-12|all}
{
  "connector.class": "io.debezium.connector.mysql.MySqlConnector",
  "database.include.list": "app_production",
  "table.include.list":    "app_production.orders,app_production.orders_invoices,…",
  "snapshot.mode":         "schema_only",
  "decimal.handling.mode": "double",
  "time.precision.mode":   "connect",
  "transforms":            "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.delete.handling.mode": "rewrite",
  "transforms.unwrap.drop.tombstones":      "true",
  "binlog.buffer.size":    "131072"
}
```

<!-- Click through: connector source → table list → snapshot/typing → tombstone handling → buffer. Five clicks, fifteen seconds. Decimal mode = double is the gotcha that loses precision but parses; we'll come back to it in the JSON-as-bytes war story. -->

---
layout: default
---

# Kafka Engine → Materialized View → ReplacingMergeTree

Three CREATE statements per table — that's the whole pattern.

1. **`<table>_kafka`** — Kafka engine table, reads from Redpanda topic, everything `Nullable`
2. **`analytics_<table>`** — ReplacingMergeTree, the queryable destination, owns `_version` + `_deleted`
3. **`<table>_mv`** — Materialized view that types, coalesces, and inserts into (2)

The MV is also the **PII firewall** — we'll come back to that on slide 13.

<!-- The MV is the workhorse — it's also our PII firewall (slide 13). Set up the three-CREATE pattern verbally before showing it. Note: `__deleted` arrives as the literal string 'true'/'false', timestamps as epoch ms — the MV cleans both. Next slide is the actual code. -->

---
layout: default
class: zoom-code
---

# Kafka Engine → Materialized View → ReplacingMergeTree
## — the three CREATE statements

```sql {1-7|9-15|17-23|all}
CREATE TABLE orders_kafka (
  id UInt64, workspace_id UInt64, total_amount Nullable(Float64),
  created_at Int64, updated_at Int64, __deleted Nullable(String)
) ENGINE = Kafka SETTINGS
  kafka_broker_list = '${REDPANDA_BROKERS}',
  kafka_topic_list  = 'datalake.app.orders',
  kafka_format      = 'JSONEachRow';

CREATE TABLE analytics_orders (
  id UInt64, workspace_id UInt64, total_amount Float64,
  created_at DateTime64(3), updated_at DateTime64(3),
  _version UInt64, _deleted UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (workspace_id, id);

CREATE MATERIALIZED VIEW orders_mv TO analytics_orders AS
SELECT id, workspace_id,
  coalesce(total_amount, 0)              AS total_amount,
  fromUnixTimestamp64Milli(created_at)   AS created_at,
  fromUnixTimestamp64Milli(updated_at)   AS updated_at,
  intDiv(updated_at, 1000)               AS _version,
  if(__deleted = 'true', 1, 0)           AS _deleted
FROM orders_kafka;
```

<!-- Click reveals: Kafka source → RMT destination → MV transform → all. Highlight on the MV: coalesce handles MySQL decimal-as-string, fromUnixTimestamp64Milli handles epoch-ms timestamps, the __deleted string cast becomes the soft-delete flag. -->

---
layout: image-right
image: /graphics/g04_bootstrap.png
---

# We Did NOT Load 9 Billion Rows Through Debezium

**WAR STORY**

- Snapshotting history through CDC = days of replica load + connector risk
- Instead: **restore RDS snapshot → export to Parquet on S3 → bulk-load into ClickHouse**
- Then start CDC at the binlog position for the delta — best of both worlds
- Result: 80B rows of history loaded in **hours, not days** — primary never felt it

<!-- This is the headline takeaway. Debezium is for the *stream*, not for hauling history. Snapshot→Parquet→bulk-insert decouples the cold start from the live pipeline and never touches the production primary. -->

---
layout: image-right
image: /graphics/g05_tombstone.png
---

# A Delete Is Just Another Event

**WAR STORY** — Naïve consumers either drop real deletes or choke on tombstones (null-value records).

```json
"transforms.unwrap.delete.handling.mode": "rewrite",
"transforms.unwrap.drop.tombstones":      "true"
```

…and in the materialized view:

```sql
if(__deleted = 'true', 1, 0) AS _deleted
```

Result: deletes become `_deleted = 1` — history kept, churned rows still queryable.

<!-- Explain what a tombstone is for the half of the room that's never hit it. Soft-delete means we can still report on churned/cancelled rows. -->

---
layout: default
---

# Two Ways Dedup Silently Fails

**WAR STORY** — Mutable ORDER BY columns. Cross-partition rows. Either one quietly keeps duplicates. Found via row-count drift: **12.68% extra rows** on one table.

```sql
-- Before: ORDER BY uses a column that can change after insert → BOTH versions kept
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (site_id, id)              -- site_id was being reassigned

-- Fix: rebuild with immutable-only sort key, swap atomically
CREATE TABLE analytics_courses_new (...)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (id);                      -- id never changes

INSERT INTO analytics_courses_new SELECT * FROM analytics_courses;
OPTIMIZE TABLE analytics_courses_new FINAL;
EXCHANGE TABLES analytics_courses AND analytics_courses_new;
```

**Rules:** only immutable columns in `ORDER BY` · never partition on anything CDC can mutate.

<!-- RMT dedups *within a partition, by sort key*. Violate either and it quietly keeps duplicates. The 12.68% number is real and required a full reload. -->

---
layout: default
---

# The Connector That Lied About Being Fine

**WAR STORY** — One large MySQL transaction overflowed `binlog.buffer.size`. Connector said RUNNING. Offsets froze.

```json
GET /connectors/orders-connector/status
{
  "connector": { "state": "RUNNING" },
  "tasks":     [{ "state": "RUNNING", "trace": null }]
}
// debezium.offsets topic: same binlog position for 6+ hours
```

```ini
binlog.buffer.size = 131072   ; 16 KB default → 128 KB
max.batch.size     = 2048
max.queue.size     = 8192
```

- Shared binlog stream → one stuck transaction stalled **every** workspace connector
- Health = **data moving**, not an API status field
- Compare connector binlog position vs `SHOW MASTER STATUS` — that's the truth

<!-- The scariest failures are the silent ones. Health = data moving, not an API saying "RUNNING". The fix script: poll connector offsets against MySQL master status; alert on files-behind, not on state strings. -->

---
layout: default
---

# When NULL Isn't False

**WAR STORY** — Rails booleans have three states: `0 (false)` · `1 (true)` · `NULL (legacy/unset)`. We assumed NULL meant not-anonymous. It didn't. **+620M rows backfilled.**

```sql
-- The bug: pulled in every NULL-anonymous legacy contact
WHERE anonymous = 0

-- Worse: `anonymous` is a generated column lazily synced from email/phone.
-- Even = 0 lied for newly-inserted rows.

-- Fix: filter on the *source* identity fields, not the derived flag
SELECT id, workspace_id, ...
FROM mysql(app_production, table='contacts')
WHERE (email_address IS NOT NULL AND email_address != '')
   OR (phone_number  IS NOT NULL AND phone_number  != '');
```

`mysql()` federation doesn't push down `ORDER BY` / `LIMIT` — treat it as a full table scan and chunk by `id` range yourself.

<!-- CDC faithfully replicates your source's quirks. Know your application's data semantics, not just the column type. Also drop the federation note: `mysql()` doesn't push down ORDER BY/LIMIT — treat it as a full scan. -->

---
layout: default
---

# PII Never Reaches the Analytics Tables

PII is excluded **at Debezium** — sensitive bytes never enter the topic. The MV is the second hard boundary: explicit column list, no `SELECT *`.

```json
"column.exclude.list":
  "app.orders.shipping_address_first_name,
   app.orders.shipping_address_last_name,
   app.orders.shipping_address_phone_number,
   app.orders.billing_address_street_one,
   app.orders.billing_address_street_two,
   app.orders.phone_number,
   app.orders.notes,
   app.orders.encryption_key"
```

```sql
-- Contacts MV: PII → presence flags only
SELECT id, workspace_id,
  if(email_address  != '', 1, 0) AS has_email,
  if(phone_number   != '', 1, 0) AS has_phone,
  if(first_name     != '', 1, 0) AS has_first_name,
  ...
FROM contacts_kafka;
```

Analysts get rich behavior. Sensitive values simply don't exist downstream.

<!-- This is privacy-by-construction. There's no "remember to mask" — the data physically isn't there. Compliance and engineering both relax. -->

---
layout: image-right
image: /graphics/g08_mcp_gateway.png
---

# Letting AI Agents Query the Lake — Safely

- **ScaleDB MCP:** read-only SQL gateway for AI agents
- GitHub OAuth org auth · SELECT-only · whitelisted tables · audit logging
- **Blocks PII tables outright** (users, contacts, memberships)
- Agents get analytics power; they **cannot** read sensitive data — even by accident

<!-- As AI agents touch internal data, the access layer is the control plane. We assume the agent is curious and untrusted; the gateway enforces the rules. -->

---
layout: default
---

# Proving the Lake Matches the Source

Cheap fingerprint per ID batch — find where to look *before* doing row comparisons.

```sql
-- Per-batch fingerprint, run against source MySQL and target in parallel
SELECT
  COUNT(*),
  COALESCE(SUM(id), 0)                          AS id_sum,
  COALESCE(SUM(UNIX_TIMESTAMP(created_at)), 0)  AS created_sum,
  COALESCE(SUM(UNIX_TIMESTAMP(updated_at)), 0)  AS updated_sum
FROM orders
WHERE id BETWEEN :batch_min AND :batch_max
  AND updated_at < :fence_time;   -- ignore in-flight CDC lag
```

- **CRC scan** all tables → list mismatched batches in minutes
- **Deep scan** only bad batches → row-by-row column compare
- **Fix mode**: `REPLACE INTO` target from source; re-verify
- Time fence makes results deterministic during live replication

<!-- Drift is inevitable at billions of rows. The trick is a cheap fingerprint that finds *where* to look before doing expensive row comparisons. Time fence: only compare rows older than script start time, so in-flight CDC lag doesn't cause false positives. -->

---
layout: image-right
image: /graphics/g10_results_bars.png
---

# What We Got

- Query speed: minutes → seconds
- Cold-start: days → hours
- Lag: minutes → seconds

<!-- Tie each result back to a decision. Speed = ClickHouse + RMT; cold start = Parquet bootstrap; safety = MV firewall + MCP gateway. -->

---
layout: default
---

# If You Build One of These

- Use **CDC for the stream**, but **snapshots for history**
- Never use mutable columns in your **ORDER BY**
- Monitor connector **offsets**, not just API status
- Treat the Materialized View as a **hard PII firewall**
- Verify integrity with **checksums**, not just row counts

<!-- This is the slide people photograph. Keep it crisp. -->

---
layout: end
---

# Questions?

Javier Zon · Founder, ScaleDB

`support@scaledb.io`

[scaledb.io](https://scaledb.io) · [github.com/scaledb-io/scaledb](https://github.com/scaledb-io/scaledb)

<!-- Invite questions, then the marketing close: point people to scaledb.io and announce that the ScaleDB binary is now open source — they can self-host the exact pipeline from this talk. The two URLs are the call to action. -->
