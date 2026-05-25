# Streaming MySQL Changes to ClickHouse

Slidev deck for the Percona Live 2026 (USA) talk:
**Streaming MySQL Changes to ClickHouse — Designing an End-to-End CDC Pipeline**

Javier Zon · Founder, ScaleDB

## Develop

```bash
npm install
npm run dev          # http://localhost:3030
```

`d` toggles dark mode during the talk. Light is the default (projector-friendly).

## Build

```bash
npm run build                # ./dist (root-served)
npm run build:pages          # ./dist with /percona-live-2026/ base for GitHub Pages
npm run export:pdf           # percona-live-2026.pdf
```

## Deploy

Pushing to `main` runs `.github/workflows/deploy.yml`, which builds the deck and
publishes it to the `gh-pages` branch. GitHub Pages serves it at
<https://scaledb-io.github.io/percona-live-2026/>.

## Speaker notes

Each slide carries its speaker notes in a `<!-- notes -->` block at the bottom of
its section. Use Slidev's presenter view (`p` from the dev server) to see them
during the talk.
