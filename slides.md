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

<!--
(~25s)
Hi everyone — I'm Javier Zon, founder of ScaleDB. For the last couple of years we've been streaming every row change from a production MySQL fleet into a ClickHouse data lake — 80 billion events, 35 terabytes, live. Today I want to walk you through the architecture, and more importantly, the scars we earned building it.
→ Next: a bit about me, then why we had to build this.
-->

---
layout: default
class: bio
---

# Javier Zon

## Founder, ScaleDB

- Database and platform engineer with **15+ years** working on MySQL, distributed systems, and real-time data infrastructure
- Former **Percona Remote DBA**, helping operate and troubleshoot production database systems at scale
- Focused on modern data architectures combining **MySQL, CDC pipelines, streaming platforms, and ClickHouse**

*Still occasionally surprised by what people run directly against production MySQL.*

<div class="bio-links">
  <a href="https://scaledb.io">scaledb.io</a>
  <span class="dot">·</span>
  <a href="https://www.linkedin.com/in/javierzon">linkedin.com/in/javierzon</a>
  <span class="dot">·</span>
  <a href="https://github.com/jtomaszon">github.com/jtomaszon</a>
</div>

<!--
(~30s)
Quick context before we dive in. I'm Javier — fifteen years on MySQL and the platforms around it, including a stint on the Percona Remote DBA team, where you see every way a production database can go sideways. These days I run ScaleDB, focused on modern data architectures that combine MySQL with CDC, streaming, and ClickHouse. Everything you'll see today comes from running that pattern in production — including, occasionally, getting surprised by what people still run directly against the source database.
→ Next: the problem that pushed us to build this.
-->

---
layout: default
---

# Analytics Was Killing the Source Database

- BI + dashboards ran against MySQL read replicas — slow, fragile, contended
- Billions of events; cross-domain joins (orders × contacts × billing) timed out
- We needed **fresh** analytics without touching production write paths
- Goal: real-time CDC into a columnar store built for analytics

<!--
(~40s)
Analytics was killing the source database. We were running BI and dashboards against MySQL read replicas — slow, fragile, and constantly contended. Cross-domain joins across orders, contacts, and billing were timing out at billions of rows. We needed fresh analytics without ever touching the production write path. The goal was real-time CDC into a columnar store built for this job.
→ Next: the scale we're actually moving.
-->

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
    <div class="stat-number">~$4k</div>
    <div class="stat-label">Full production lake cost/mo</div>
  </div>
</div>

<!--
(~45s)
Quick sense of scale before we dig in. 80 billion+ events landed, 35 terabytes of analytical data, 64 Redpanda partitions at RF=3 carrying the stream. And the punchline: the full production lake costs us around four thousand dollars a month — less than a few oversized RDS instances. That's the budget number to hold onto.
→ Next: how the whole pipeline fits together.
-->

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

<!--
(~75s)
Let me walk one row change end to end. A write hits MySQL — Debezium reads it from the binlog, never from a table. The change lands in Redpanda, partitioned by table, replicated three ways. ClickHouse pulls it through a Kafka engine table, a materialized view types and filters it — that MV is also our PII firewall — and it lands in a ReplacingMergeTree where BI tools and AI agents read it. The key word is loose coupling: Redpanda is a buffer, so if ClickHouse goes down for an hour, we lose nothing.
→ Next: the four decisions that shaped everything downstream.
-->

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

<!--
(~60s)
Four decisions, all made before we wrote a single line of pipeline code. Get any one of them wrong and the whole thing falls over. ReplacingMergeTree with a version column gives us last-writer-wins dedup — without it CDC creates duplicates on every restart. Soft deletes mean a `DELETE` becomes a flag, so we can still report on churned rows. Read-only and PII-safe by construction means sensitive columns physically don't exist downstream — no "remember to mask." Buffer, don't couple — Redpanda absorbs anything MySQL throws at it. Watch for these four; every war story I'm about to tell is one of them paid for in production.
→ Next: capturing change without re-reading MySQL.
-->

---
layout: default
---

# Capturing Change Without Re-Reading MySQL

- One Debezium connector **per domain** (orders, contacts, products, …)
- `snapshot.mode: schema_only` → start at current binlog, never resnapshot history
- Redpanda (Kafka API): **3 nodes, RF=3, 64 partitions**, topic per table
- Why Redpanda: no ZooKeeper, simpler ops, NVMe nodes

The connector config is short — six settings do the real work.

<!--
(~45s)
One Debezium connector per domain — orders, contacts, products — so a stuck connector blasts only one workspace, not all of them. `snapshot.mode: schema_only` means we never resnapshot history; we start at the current binlog position. Redpanda gives us the Kafka API with no ZooKeeper, simpler ops, NVMe-backed nodes. Six settings do the real work — and they're on the next slide.
→ Next: the actual connector config.
-->

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

<!--
(~75s)
Five clicks. First: the MySQL connector. Then the table list, scoped per domain. `schema_only` plus `decimal.handling.mode: double` — that's the typing line; double loses a hair of precision but parses cleanly, where the default emits bytes. Then the unwrap transform with `delete.handling.mode: rewrite` and `drop.tombstones: true` — that converts deletes into a flag we control, and we'll come back to that in two slides. Finally the binlog buffer at 128k, which has its own war story coming up.
→ Next: how the data lands in ClickHouse.
-->

---
layout: default
---

# Kafka Engine → Materialized View → ReplacingMergeTree

Three CREATE statements per table — that's the whole pattern.

1. **`<table>_kafka`** — Kafka engine table, reads from Redpanda topic, everything `Nullable`
2. **`analytics_<table>`** — ReplacingMergeTree, the queryable destination, owns `_version` + `_deleted`
3. **`<table>_mv`** — Materialized view that types, coalesces, and inserts into (2)

The MV is also the **PII firewall** — we'll come back to that shortly.

<!--
(~45s)
Three CREATE statements per table — that's the entire ClickHouse pattern. A Kafka engine table reading raw JSON from Redpanda, with everything Nullable because the source is messy. A ReplacingMergeTree with strict types — that's the queryable destination, owning `_version` and `_deleted`. And a materialized view in between that types, coalesces, and crucially acts as our PII firewall. We'll come back to that firewall later.
→ Next: the three CREATE statements, side by side.
-->

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

<!--
(~90s)
Top: the Kafka engine reads JSON, everything Nullable because we trust nothing yet. Middle: the RMT, strict types, partitioned by month, sorted by workspace and id — and notice `_version` is just `updated_at` in seconds, so last-writer-wins is automatic. Bottom is where the work happens: `coalesce` fills in defaults, `fromUnixTimestamp64Milli` converts epoch-ms back to real timestamps, and the literal string `'true'` from `__deleted` becomes a 1. Three statements, repeated per table. That's the whole pattern.
→ Next: the first war story — bootstrapping 9 billion rows.
-->

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

<!--
(~90s)
If you take one thing from this talk, take this: CDC is for the stream, not for hauling history. Our first instinct was to let Debezium snapshot nine billion rows. It would have taken days, hammered the replica, and any connector hiccup mid-run meant starting over. So we split it. Take an RDS snapshot — a point-in-time copy that doesn't touch prod. Export it to Parquet on S3, columnar and compressed. Bulk-load straight into the ReplacingMergeTree — 80 billion rows in hours. Then start CDC from the binlog position the snapshot was taken at, and the delta catches up in minutes. The production primary never felt it.
→ Next: deletes — they're not what you think.
-->

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

<!--
(~75s)
A delete in MySQL becomes a Kafka tombstone — same key, null value — and naive consumers either drop the delete entirely or crash on the null. Debezium's `unwrap` transform with `delete.handling.mode: rewrite` turns that into a real event with `__deleted = 'true'`, and we tell it to drop the actual tombstone. The MV maps that string to a `_deleted` flag. The row stays in ClickHouse, queries filter it out by default — but we can still report on churned customers and cancelled orders, because the history is intact.
→ Next: how RMT silently keeps duplicates.
-->

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

<!--
(~60s)
We learned this one the hard way. ReplacingMergeTree dedups within a partition, by sort key — violate either condition and it quietly keeps both copies. First failure: a mutable column in your ORDER BY — if it changes after insert, you get two rows with different sort keys and RMT thinks they're different rows. Second: cross-partition duplicates — RMT never dedups across partitions. We caught it by row-count drift: 12.68% extra rows on one table. Full reload required. The rule: only immutable columns in ORDER BY, and never partition on anything CDC can mutate.
→ Next: the actual rebuild we ran in production.
-->

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

<!--
(~75s)
Top: the broken table — `site_id` was being reassigned by an internal process, so the sort key kept shifting. Middle: the rebuild, sorted by `id` only, which never changes. Bottom: the atomic swap — INSERT into the new table, OPTIMIZE FINAL to collapse duplicates one last time, then EXCHANGE TABLES to flip them in a single metadata operation. One trick we learned: drop the MV before the INSERT so CDC buffers in Redpanda during the rebuild, then recreate it after the swap. Zero data loss.
→ Next: the connector that lied about being healthy.
-->

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

<!--
(~60s)
The scariest failures are the silent ones. One unusually large MySQL transaction overflowed Debezium's binlog buffer — and the connector dutifully reported RUNNING. Offsets froze for six hours. Because connectors share the binlog stream, one stuck transaction stalled every workspace at once. The lesson: health is data moving, not an API status field. We now compare connector binlog position against `SHOW MASTER STATUS` — that's the truth. The fix was 16k to 128k on the buffer, plus per-partition offset monitoring.
→ Next: what RUNNING actually looked like, and the fix.
-->

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

<!--
(~75s)
Top: the status API. Both `connector` and `tasks` say RUNNING, `trace` is null — looks perfect. There's no exception path inside Debezium for "I'm internally stuck on a buffer," so the status field just stays green. The line below is what told us the truth: the offsets topic showed the same binlog position for six hours. Bottom is the fix — bump the binlog buffer to 128k, raise batch and queue sizes for headroom on similar bursts. Buffer size was the real fix; the others are insurance.
→ Next: the boolean that added 620 million rows.
-->

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

<!--
(~60s)
Rails booleans have three states: zero, one, and NULL for legacy or unset rows. We assumed NULL meant "not anonymous" and backfilled — that added 620 million extra rows. Worse: the `anonymous` column is a generated column lazily synced from email and phone, so even `= 0` lied for newly-inserted rows. CDC faithfully replicates your source's quirks; you have to know the application's data semantics, not just the column type. The fix was to filter on the source identity fields directly. Bonus: `mysql()` federation doesn't push down ORDER BY or LIMIT — treat it as a full scan and chunk by id range yourself.
→ Next: the buggy query and the fix.
-->

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

<!--
(~75s)
Top: the obvious-looking bug — `WHERE anonymous = 0` looks fine, but it pulled in every legacy contact with NULL. Middle: the deeper gotcha — `anonymous` is generated and lazily synced, so it's stale on newly-inserted rows too. Bottom is the correct filter: check the actual identity fields, email and phone. They're authoritative; the derived flag is downstream of them. Rule of thumb: never trust a flag if you can check what it's derived from.
→ Next: how we keep PII out of analytics entirely.
-->

---
layout: default
---

# PII Never Reaches the Analytics Tables

Two hard boundaries, both enforced **before** any analyst sees a row:

- **At the connector** — Debezium's `column.exclude.list` drops PII columns from the binlog stream. Sensitive bytes never enter the topic.
- **At the materialized view** — explicit column list, no `SELECT *`. PII columns can't accidentally land downstream even if the connector misses one.

Contacts are identified by **presence** of email/phone, never by the values. Analysts get rich behavior; sensitive fields physically don't exist downstream.

<!--
(~45s)
You can't leak data you never stored. That's the whole idea behind this slide. We enforce PII safety at two boundaries, and there's no human between them. First boundary: Debezium's `column.exclude.list` strips sensitive columns out of the binlog stream before they ever hit Redpanda. Second boundary: every materialized view has an explicit column list — no `SELECT *` — so even if the first boundary misses one, the MV catches it. Contacts are identified by the presence of email or phone, never by the values themselves. Compliance stopped asking us to mask things; the things just aren't there.
→ Next: the two boundaries side by side, in code.
-->

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

<!--
(~60s)
Left: the connector boundary. `column.exclude.list` strips PII at the source — names, addresses, phone numbers, encryption keys — those bytes never enter the topic. Right: the materialized view boundary. Even on tables where we have to ingest a column, the MV maps it to a presence flag — `has_email`, `has_phone`. Two independent defenses; either one alone would have been enough, but together they mean a connector misconfiguration doesn't become a compliance incident.
→ Next: letting AI agents query this lake safely.
-->

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

<!--
(~90s)
Quick framing before I describe this: we didn't build the MCP gateway because AI is fashionable. We built it because analysts and internal tooling needed safe read access to the lake, and "give them a Postgres user" wasn't going to fly. Once that gateway exists, agents are just another client. The threat model is the same: curious and untrusted. ScaleDB MCP sits between any client and ClickHouse — GitHub OAuth for org-scoped identity, SELECT-only by enforcement, table whitelist, every query audited. PII tables — users, contacts, memberships — are blocked outright. The result: agents get the full analytical power of the lake, and they cannot read sensitive data even by accident, because the path to that data doesn't exist for them.
→ Next: proving the lake matches the source.
-->

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

<!--
(~45s)
Drift is inevitable at billions of rows — the trick is finding where to look before you compare anything expensive. We run a cheap CRC scan across every table to surface mismatched ID batches in minutes. Then a deep scan only on those bad batches, row by row. A fix mode does REPLACE INTO from source and re-verifies. The critical trick: time-fence every query to before script start, so in-flight CDC lag doesn't generate false positives.
→ Next: the actual fingerprint query.
-->

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

<!--
(~60s)
Same query, both sides, in parallel. Four aggregates per batch: count, sum of ids, sum of created timestamps, sum of updated timestamps. Sum-of-ids catches missing or extra rows; sum-of-timestamps catches stale updates the count would miss. The batch range keeps each query bounded. And that last line — the time fence — is the magic: both sides see the same logical snapshot regardless of replication lag. Two ints and two longs per batch, runs across a billion rows in minutes.
→ Next: what all of this got us.
-->

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

All of it on **~$4k/month** — for a full real-time lake of 80B+ events.

<!--
(~60s)
Was it worth it? Three numbers say yes. Analysts used to wait minutes for a dashboard to load — now they get answers in seconds. Cold-starting the lake used to be a multi-day project that risked taking down the primary — now it's afternoon work that never touches production. And CDC lag went from minutes to seconds, which is the difference between "this dashboard is broken" and "this dashboard is live." Each one maps to one of the four decisions: ClickHouse plus RMT, the Parquet bootstrap, and Redpanda buffering. And the whole thing runs for about $4,000 a month — less than a few oversized RDS instances.
→ Next: but stuff still breaks — how we hear about it.
-->

---
layout: default
class: ops-sponsor
---

# Things Go Wrong. Plan for It.

<div class="pipeline">
  <div class="lane">
    <div class="lane-label">Metrics</div>
    <div class="node">CloudWatch<br/><span class="hint">binlog lag · freshness · offsets</span></div>
  </div>
  <div class="connector"></div>
  <div class="lane">
    <div class="lane-label">Alarms</div>
    <div class="node">Composite<br/><span class="hint">AnyConnectorLag · 5 files / 5 min → crit at 20 / 30 min</span></div>
  </div>
  <div class="connector"></div>
  <div class="lane">
    <div class="lane-label">Routing</div>
    <div class="node">SNS topic<br/><span class="hint">Events API v2</span></div>
  </div>
  <div class="connector"></div>
  <div class="lane">
    <div class="lane-label">On-call</div>
    <div class="node accent">PagerDuty<br/><span class="hint">runbook URL in payload</span></div>
  </div>
</div>

- Every node pushes pipeline metrics to CloudWatch — binlog lag, freshness, offsets
- Per-connector + a composite "any connector behind" alarm
- Runbook URL travels **with** the alert — on-call gets context, not just a red number

<div class="sponsor-strip">
  <div class="sponsor-text">
    Thanks to <strong>PagerDuty</strong> — our sponsor today, and the layer that wakes us up when this thing actually breaks.
  </div>
  <img src="/qr-pagerduty.svg" class="sponsor-qr" alt="github.com/PagerDuty" />
</div>

<!--
(~75s)
This is the operational layer we haven't talked about yet — what happens when one of the war stories you just heard happens again, except at 3 AM. Every node pushes metrics to CloudWatch — binlog lag, freshness, offsets per connector. We have per-connector alarms plus a composite "any connector behind" that fires the moment one of them falls over. Those alarms route through an SNS topic to PagerDuty, and the runbook URL travels with the alert payload — on-call gets context, not just a red number. And speaking of PagerDuty: they're our sponsor today, and they're also the layer that actually wakes us up when this stuff breaks. Worth a thank you.
→ Next: five lessons earned in production.
-->

---
layout: default
---

# If You Build One of These

- Use **CDC for the stream**, but **snapshots for history**
- Never use mutable columns in your **ORDER BY**
- Monitor connector **offsets**, not just API status
- Treat the Materialized View as a **hard PII firewall**
- Verify integrity with **checksums**, not just row counts

<div class="closing-line">The hard part isn't moving data. It's operating the pipeline safely at scale.</div>

<!--
(~45s)
This is the slide people photograph — pause here. If you build one of these: use CDC for the stream and snapshots for history. Never put a mutable column in your ORDER BY. Monitor offsets, not API status. Treat the MV as a hard PII firewall. And verify integrity with checksums, not row counts. Five rules — every one of them paid for in production. And the meta-lesson I'll leave you with: the hard part isn't moving data — it's operating the pipeline safely at scale.
→ Next: questions, and where to find the code.
-->

---
layout: default
class: closing
---

<div class="closing-stack">

<img src="/scaledb-logo.png" class="brand-logo" alt="ScaleDB" />

# Questions?

<div class="closing-meta">Javier Zon · Founder, ScaleDB · <code>support@scaledb.io</code></div>

<div class="cta-row">

<div class="cta-cards">
  <a href="https://scaledb.io" class="cta-card">
    <div class="cta-card-label">Learn more about us</div>
    <div class="cta-card-url">scaledb.io</div>
  </a>
  <a href="https://github.com/scaledb-io/cloud" class="cta-card">
    <div class="cta-card-label">Clone &amp; run locally · docker compose up</div>
    <div class="cta-card-url">github.com/scaledb-io/cloud</div>
  </a>
</div>

<div class="qr-block">
  <img src="/qr-cloud.svg" alt="QR — github.com/scaledb-io/cloud" />
  <div class="qr-caption">Scan for the repo</div>
</div>

</div>

<img src="/percona-live-2026-bay.png" class="event-logo-small" alt="Percona Live 26 · Bay Area" />

</div>

<!--
(~45s — then opens ~5 min Q&A)
That's the talk — I'd love your questions. Two pointers before we open it up: scaledb.io is where we live, and the entire CDC platform we just walked through is open source at github.com/scaledb-io/cloud. Clone it, run `docker compose up`, and you've got the exact pipeline from this talk running on your laptop in about ten minutes. Find me after if we run out of time. Thanks for having me.
→ Q&A.
-->
