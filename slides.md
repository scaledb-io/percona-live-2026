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
aspectRatio: '16/9'
canvasWidth: 1280
---

# Streaming MySQL Changes<br/>to ClickHouse

## Designing an End-to-End CDC Pipeline

Javier Zon · Founder, ScaleDB

<img src="/percona-live-2026-bay.png" class="event-logo" alt="Percona Live 2026 — Bay Area" />

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
layout: default
---

# What We're Actually Moving

<div class="stat-grid">
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
layout: default
---

# The End-to-End Pipeline

<div class="pipeline">
  <div class="lane">
    <div class="lane-label">OLTP</div>
    <div class="node">MySQL 8<br/><span class="hint">binlog</span></div>
  </div>
  <div class="connector"></div>
  <div class="lane">
    <div class="lane-label">CDC</div>
    <div class="node">Debezium</div>
    <div class="arrow">→</div>
    <div class="node">Redpanda<br/><span class="hint">RF=3 · 64 partitions</span></div>
  </div>
  <div class="connector"></div>
  <div class="lane">
    <div class="lane-label">OLAP — Lake</div>
    <div class="node">Kafka engine</div>
    <div class="arrow">→</div>
    <div class="node">Materialized View<br/><span class="hint">types · PII firewall</span></div>
    <div class="arrow">→</div>
    <div class="node accent">ReplacingMergeTree<br/><span class="hint">_version · _deleted</span></div>
  </div>
  <div class="connector"></div>
  <div class="lane">
    <div class="lane-label">Clients</div>
    <div class="node ghost">BI · AI agents</div>
  </div>
</div>

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

```json {2-3|4-5|6-9|10-12}
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

```sql {1-6|8-13|15-22}
CREATE TABLE orders_kafka (
  id UInt64, workspace_id UInt64, total_amount Nullable(Float64),
  created_at Int64, updated_at Int64, __deleted Nullable(String)
) ENGINE = Kafka SETTINGS
  kafka_broker_list = '${REDPANDA_BROKERS}',
  kafka_topic_list  = 'datalake.app.orders', kafka_format = 'JSONEachRow';
-- ───────────────────────────────────────────────────────────────
CREATE TABLE analytics_orders (
  id UInt64, workspace_id UInt64, total_amount Float64,
  created_at DateTime64(3), updated_at DateTime64(3),
  _version UInt64, _deleted UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(created_at) ORDER BY (workspace_id, id);
-- ───────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW orders_mv TO analytics_orders AS SELECT
  id, workspace_id,
  coalesce(total_amount, 0)            AS total_amount,
  fromUnixTimestamp64Milli(created_at) AS created_at,
  fromUnixTimestamp64Milli(updated_at) AS updated_at,
  intDiv(updated_at, 1000)             AS _version,
  if(__deleted = 'true', 1, 0)         AS _deleted
FROM orders_kafka;
```

<!-- Click reveals: Kafka source → RMT destination → MV transform → all. Highlight on the MV: coalesce handles MySQL decimal-as-string, fromUnixTimestamp64Milli handles epoch-ms timestamps, the __deleted string cast becomes the soft-delete flag. -->

---
layout: default
class: war
---

# We Did NOT Load 9 Billion Rows Through Debezium

CDC is for the stream, **not** for hauling history.

<div class="pipeline">
  <div class="lane">
    <div class="lane-label">1 · Snapshot</div>
    <div class="node">RDS snapshot<br/><span class="hint">point-in-time copy</span></div>
  </div>
  <div class="connector"></div>
  <div class="lane">
    <div class="lane-label">2 · Export</div>
    <div class="node">Parquet on S3<br/><span class="hint">columnar, compressed</span></div>
  </div>
  <div class="connector"></div>
  <div class="lane">
    <div class="lane-label">3 · Bulk-load</div>
    <div class="node accent">ClickHouse RMT<br/><span class="hint">80B rows in hours</span></div>
  </div>
  <div class="connector"></div>
  <div class="lane">
    <div class="lane-label">4 · Catch up</div>
    <div class="node ghost">CDC from binlog pos.<br/><span class="hint">delta only</span></div>
  </div>
</div>

- Snapshotting history through CDC = **days** of replica load + connector risk
- Decoupling cold-start from the live pipeline → primary never feels it
- Then start CDC at the binlog position for the delta — best of both worlds

<!-- This is the headline takeaway. Debezium is for the *stream*, not for hauling history. Snapshot→Parquet→bulk-insert decouples the cold start from the live pipeline and never touches the production primary. Hours not days, no impact on prod. -->

---
layout: default
class: war
---

# A Delete Is Just Another Event

Naïve consumers either drop real deletes or choke on tombstones (null-value records).

<div class="pipeline">
  <div class="lane">
    <div class="lane-label">MySQL</div>
    <div class="node">DELETE FROM orders<br/><span class="hint">WHERE id = …</span></div>
  </div>
  <div class="connector"></div>
  <div class="lane">
    <div class="lane-label">Kafka</div>
    <div class="node">Tombstone<br/><span class="hint">key=id · value=null</span></div>
  </div>
  <div class="connector"></div>
  <div class="lane">
    <div class="lane-label">Debezium</div>
    <div class="node">unwrap rewrite<br/><span class="hint">__deleted = 'true'</span></div>
  </div>
  <div class="connector"></div>
  <div class="lane">
    <div class="lane-label">ClickHouse</div>
    <div class="node accent">_deleted = 1<br/><span class="hint">row kept, filtered at query</span></div>
  </div>
</div>

```json
"transforms.unwrap.delete.handling.mode": "rewrite",
"transforms.unwrap.drop.tombstones":      "true"
```

History kept, churned rows still queryable.

<!-- Explain what a tombstone is for the half of the room that's never hit it. Soft-delete means we can still report on churned/cancelled rows. -->

---
layout: default
class: war
---

# Two Ways Dedup Silently Fails

RMT dedups **within a partition, by sort key**. Violate either and it quietly keeps duplicates.

- **Mutable column in `ORDER BY`** — a value that changes post-insert produces two rows with different sort keys; RMT keeps both
- **Cross-partition duplicates** — RMT never dedups across partitions; one row reinserted into a new month survives

We caught it by row-count drift: **12.68% extra rows** on one table. Full reload required.

**Rules:** only immutable columns in `ORDER BY` · never partition on anything CDC can mutate.

<!-- Set up the two failure modes verbally. Land the 12.68% as the punchline — that's the number that made us investigate. Next slide shows the actual rebuild migration. -->

---
layout: default
class: zoom-code war
---

# Two Ways Dedup Silently Fails
## — the rebuild migration (atomic swap)

```sql {1-4|6-10|12-14}
-- Before: ORDER BY uses a column that can change post-insert → BOTH versions kept
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (site_id, id)              -- site_id was being reassigned

-- Fix: rebuild with immutable-only sort key
CREATE TABLE analytics_courses_new (...)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (id);                      -- id never changes

INSERT INTO analytics_courses_new SELECT * FROM analytics_courses;
OPTIMIZE TABLE analytics_courses_new FINAL;
EXCHANGE TABLES analytics_courses AND analytics_courses_new;
```

<!-- Click reveals: before (the broken ORDER BY), after (the new table with id-only sort key), the atomic swap (INSERT + OPTIMIZE + EXCHANGE). Drop the MV first so CDC buffers in Redpanda during migration; recreate after. -->

---
layout: default
class: war
---

# The Connector That Lied About Being Fine

One large MySQL transaction overflowed `binlog.buffer.size`. The connector dutifully reported **RUNNING**. Offsets froze for 6+ hours.

- Shared binlog stream → one stuck transaction stalled **every** workspace connector
- Health is **data moving**, not an API status field
- Compare connector binlog position vs `SHOW MASTER STATUS` — that's the truth
- Fix: buffer 16 KB → 128 KB, plus monitor offsets per partition

<!-- The scariest failures are the silent ones. Walk through: large txn, buffer overflow, connector state stays RUNNING because there's no exception path. Health = data moving, not API state. Set up the next slide: what we monitor now. -->

---
layout: default
class: zoom-code war
---

# The Connector That Lied About Being Fine
## — what RUNNING really meant, and the fix

```json {1-5|7}
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

<!-- Click reveals: the misleading status JSON → the truth from the offsets topic → the tuning settings. Buffer size is the key fix; the other two are headroom for similar bursts. -->

---
layout: default
class: war
---

# When NULL Isn't False

Rails booleans have three states: `0 (false)` · `1 (true)` · `NULL (legacy/unset)`.

- We assumed `NULL = not anonymous` → backfilled **+620M extra rows**
- Worse: `anonymous` is a **generated column** lazily synced from email/phone, so even `= 0` lied for newly-inserted rows
- Fix: filter on the **source identity fields**, not the derived flag

Bonus war story: `mysql()` federation doesn't push down `ORDER BY` / `LIMIT` — treat it as a full scan and chunk by `id` range yourself.

<!-- CDC faithfully replicates your source's quirks. Know your application's data semantics, not just the column type. Three-state booleans bite Rails-stack folks especially hard. Land the +620M as the punchline, then drop the federation note as the bonus takeaway. -->

---
layout: default
class: zoom-code war
---

# When NULL Isn't False
## — the buggy query and the fix

```sql {1-2|4-5|7-11}
-- The bug: pulled in every NULL-anonymous legacy contact
WHERE anonymous = 0

-- Worse: `anonymous` is a generated column, lazily synced from email/phone.
-- Even `= 0` lied for newly-inserted rows.

-- Fix: filter on the source identity fields, not the derived flag
SELECT id, workspace_id, ...
FROM mysql(app_production, table='contacts')
WHERE (email_address IS NOT NULL AND email_address != '')
   OR (phone_number  IS NOT NULL AND phone_number  != '');
```

<!-- Click reveals: the obvious-looking bug → the deeper gotcha (generated column) → the correct filter. The fix pulls from the source identity fields because they're authoritative; the derived flag is downstream of them and can be stale. -->

---
layout: default
---

# PII Never Reaches the Analytics Tables

Two hard boundaries, both enforced **before** any analyst sees a row:

- **At the connector** — Debezium's `column.exclude.list` drops PII columns from the binlog stream. Sensitive bytes never enter the topic.
- **At the materialized view** — explicit column list, no `SELECT *`. PII columns can't accidentally land downstream even if the connector misses one.

Contacts are identified by **presence** of email/phone, never by the values. Analysts get rich behavior; sensitive fields physically don't exist downstream.

<!-- Privacy-by-construction. There's no "remember to mask" — the data physically isn't there. Compliance and engineering both relax. Next slide shows the two boundaries side by side. -->

---
layout: default
class: zoom-code
---

# PII Never Reaches the Analytics Tables
## — the two boundaries

<div class="grid grid-cols-2 gap-6">

<div>

**1 · At the connector**

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

</div>

<div>

**2 · In the materialized view**

```sql
-- Contacts MV: PII → presence flags only
SELECT id, workspace_id,
  if(email_address != '', 1, 0) AS has_email,
  if(phone_number  != '', 1, 0) AS has_phone,
  if(first_name    != '', 1, 0) AS has_first_name,
  ...
FROM contacts_kafka;
```

</div>

</div>

<!-- Side-by-side: the connector boundary on the left strips PII at the source; the MV on the right is a belt-and-suspenders second layer that maps remaining PII to presence flags. Two independent defenses. -->

---
layout: default
---

# Letting AI Agents Query the Lake — Safely

<div class="pipeline">
  <div class="lane">
    <div class="lane-label">Client</div>
    <div class="node">AI agent<br/><span class="hint">Claude · GPT · custom</span></div>
  </div>
  <div class="connector"></div>
  <div class="lane">
    <div class="lane-label">Auth</div>
    <div class="node">GitHub OAuth<br/><span class="hint">org-scoped</span></div>
  </div>
  <div class="connector"></div>
  <div class="lane">
    <div class="lane-label">Gateway</div>
    <div class="node accent">ScaleDB MCP<br/><span class="hint">SELECT-only · whitelist · audit</span></div>
  </div>
  <div class="connector"></div>
  <div class="lane">
    <div class="lane-label">Data</div>
    <div class="node">Analytics tables<br/><span class="hint">PII tables blocked</span></div>
  </div>
</div>

- The access layer is the **control plane** — assume the agent is curious and untrusted
- SELECT-only · whitelisted tables · every query audited
- **Blocks PII tables outright** (users, contacts, memberships)
- Agents get analytics power; they **cannot** read sensitive data — even by accident

<!-- As AI agents touch internal data, the access layer is the control plane. We assume the agent is curious and untrusted; the gateway enforces the rules. -->

---
layout: default
---

# Proving the Lake Matches the Source

Drift is inevitable at billions of rows. The trick: find **where** to look before comparing rows.

- **CRC scan** all tables → list mismatched ID batches in minutes
- **Deep scan** only the bad batches → row-by-row column compare
- **Fix mode**: `REPLACE INTO` target from source; re-verify
- **Time fence** all queries to before script-start, so in-flight CDC lag doesn't cause false positives

Cheap fingerprint, expensive only where it matters.

<!-- The fingerprint is just count + ID sum + timestamp sums per ID batch — runs against source MySQL and target in parallel. Mismatched batches get the deep scan. Time fence is the key trick: ignore rows updated after script start time, so we compare the same snapshot on both sides. -->

---
layout: default
class: zoom-code
---

# Proving the Lake Matches the Source
## — the per-batch fingerprint

```sql {1|3-6|8|9}
-- Run against source MySQL and target ClickHouse in parallel
SELECT
  COUNT(*),
  COALESCE(SUM(id), 0)                          AS id_sum,
  COALESCE(SUM(UNIX_TIMESTAMP(created_at)), 0)  AS created_sum,
  COALESCE(SUM(UNIX_TIMESTAMP(updated_at)), 0)  AS updated_sum
FROM orders
WHERE id BETWEEN :batch_min AND :batch_max
  AND updated_at < :fence_time;   -- ignore in-flight CDC lag
```

Four cheap aggregates per batch. If any disagree → schedule deep scan. The time fence is the trick: both sides see the same snapshot regardless of replication lag.

<!-- Click reveals: header comment → the four aggregates → batch range → time fence. Sum-of-ids catches missing/extra rows; sum-of-timestamps catches stale updates. Two ints + two longs per batch — tiny network cost, runs across a billion-row table in minutes. -->

---
layout: default
---

# What We Got

<div class="results">
  <div class="result-row">
    <div class="result-label">Query speed</div>
    <div class="result-bars">
      <div class="bar before" style="width: 90%"><span>minutes</span></div>
      <div class="bar after"  style="width: 12%"><span>seconds</span></div>
    </div>
  </div>
  <div class="result-row">
    <div class="result-label">Cold-start</div>
    <div class="result-bars">
      <div class="bar before" style="width: 90%"><span>days</span></div>
      <div class="bar after"  style="width: 18%"><span>hours</span></div>
    </div>
  </div>
  <div class="result-row">
    <div class="result-label">CDC lag</div>
    <div class="result-bars">
      <div class="bar before" style="width: 90%"><span>minutes</span></div>
      <div class="bar after"  style="width: 8%"><span>seconds</span></div>
    </div>
  </div>
</div>

Speed = ClickHouse + RMT · Cold-start = Parquet bootstrap · Safety = MV firewall + MCP gateway.

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
layout: default
class: closing
---

<div class="closing-stack">

<img src="/scaledb-logo.png" class="brand-logo" alt="ScaleDB" />

# Questions?

<div class="closing-meta">Javier Zon · Founder, ScaleDB · <code>support@scaledb.io</code></div>

<div class="cta-cards">
  <a href="https://scaledb.io" class="cta-card">
    <div class="cta-card-label">Learn more about us</div>
    <div class="cta-card-url">scaledb.io</div>
  </a>
  <a href="https://github.com/scaledb-io/cloud" class="cta-card">
    <div class="cta-card-label">Open source CDC platform</div>
    <div class="cta-card-url">github.com/scaledb-io/cloud</div>
  </a>
</div>

<img src="/percona-live-2026-bay.png" class="event-logo-small" alt="Percona Live 26 · Bay Area" />

</div>

<!-- Invite questions, then the marketing close: point people to scaledb.io and announce that the ScaleDB binary is open source — they can self-host the exact pipeline from this talk. The two URLs are the call to action. -->
