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
layout: image-right
image: /graphics/g03_fanout.png
---

# Capturing Change Without Re-Reading MySQL

- Debezium reads the **binlog** — one connector per domain
- `snapshot.mode = schema_only` → start at current binlog, capture only new changes
- Redpanda (Kafka API): 3 nodes, RF=3, 64 partitions, topic per table
- One durable log → many consumers, replayable

Why Redpanda: no ZooKeeper, simpler ops, NVMe nodes.

<!-- Why Redpanda over Kafka: simpler ops, no ZooKeeper, NVMe nodes. schema_only is the setup for the next big story (we bootstrap history differently). -->

---
layout: image-right
image: /graphics/g02_ch_internal.png
---

# Kafka Engine → Materialized View → ReplacingMergeTree

- MV does the typing & coalescing (MySQL decimals arrive as strings!)
- `_version` lets `FINAL` collapse to the latest row at query time

The MV is also the PII firewall — see slide 13.

<!-- The MV is the workhorse — it's also our PII firewall (slide 13). -->

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

**WAR STORY**

- Kafka compaction emits **tombstones** (null-value records) on delete
- Naïve consumers either drop real deletes or choke on nulls
- Config fix: `delete.handling.mode = rewrite` · `drop.tombstones = true`
- Deletes become `_deleted = 1` → analytics keep history, queries filter it out

<!-- Explain what a tombstone is for the half of the room that's never hit it. Soft-delete means we can still report on churned/cancelled rows. -->

---
layout: image-right
image: /graphics/g06_rmt_dedup.png
---

# Two Ways Dedup Silently Fails

**WAR STORY**

- **Mutable columns in ORDER BY:** changing `user_id` made RMT keep BOTH rows
- **Cross-partition dedup doesn't exist:** duplicates across months
- **RULES:** only immutable columns in ORDER BY; never partition on anything CDC can change

<!-- RMT dedups *within a partition, by sort key*. Violate either and it quietly keeps duplicates. The 12.68% number is real and required a full reload. -->

---
layout: default
---

# The Connector That Lied About Being Fine

**WAR STORY**

- A large MySQL transaction overflowed `binlog.buffer.size` (16 KB default)
- Connector showed **RUNNING** — but committed zero offsets, lag grew forever
- Shared binlog stream → one bad transaction stalled **every** workspace connector
- Fix: buffer 16 KB → 128 KB; monitor **offset progress**, never status alone

> API Status: **RUNNING** · Actual Lag: **∞ (0 offsets)**
> Monitor progress, not status

<!-- The scariest failures are the silent ones. Health = data moving, not an API saying "RUNNING". -->

---
layout: default
---

# When NULL Isn't False

**WAR STORY**

Rails booleans have three states: `0 (false)` · `1 (true)` · `NULL (legacy/unset)`

**+620M rows backfilled**

- Rails booleans have three states: 0, 1, NULL (legacy/unset)
- We assumed NULL = not anonymous → backfilled **~620M** extra rows
- Worse: a lazily-synced generated column meant even `= 0` was wrong
- Fix: filter on source fields (email/phone), not derived flags

`mysql()` federation doesn't push down ORDER BY/LIMIT — treat as full scan.

<!-- CDC faithfully replicates your source's quirks. Know your application's data semantics, not just the column type. Also drop the federation note: `mysql()` doesn't push down ORDER BY/LIMIT — treat it as a full scan. -->

---
layout: image-right
image: /graphics/g07_pii_firewall.png
---

# PII Never Reaches the Analytics Tables

- PII columns (names, addresses, phone, emails) **excluded at Debezium**
- The **Materialized View** is a hard boundary — selects only safe columns
- Contacts identified by presence of email/phone, but values aren't stored raw
- Analysts query rich behavior; sensitive fields simply don't exist downstream

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
layout: image-right
image: /graphics/g09_integrity.png
---

# Proving the Lake Matches the Source

- `verify-cdc-integrity.rb`: **CRC scan** (count + checksums per ID batch)
- Mismatch? **Deep scan** only the bad batches (row-by-row)
- **Fix** mode repairs from MySQL; re-verify — never trust row counts alone
- Time-fenced to ignore in-flight CDC lag

<!-- Drift is inevitable at billions of rows. The trick is a cheap fingerprint that finds *where* to look before doing expensive row comparisons. -->

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
