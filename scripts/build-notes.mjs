#!/usr/bin/env node
// Generates public/notes.html — a mobile-friendly vertical scroll of every
// slide's title + speaker note. Built from slides.md.
//
// Runs automatically before the GitHub Pages build (see package.json).

import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const repoRoot  = path.resolve(__dirname, '..')
const slidesMd  = path.join(repoRoot, 'slides.md')
const outFile   = path.join(repoRoot, 'public', 'notes.html')

const md = fs.readFileSync(slidesMd, 'utf-8')

// Walk the file, find every H1 (each marks a slide). For each slide, capture
// title + the trailing <!-- ... --> note block (if any).
const h1re = /^# (.+?)$/gm
const slides = []
const starts = []
let m
while ((m = h1re.exec(md)) !== null) starts.push({ index: m.index, title: m[1] })

for (let i = 0; i < starts.length; i++) {
  const start = starts[i].index
  const end   = i + 1 < starts.length ? starts[i + 1].index : md.length
  const chunk = md.slice(start, end)

  const title = starts[i].title
    .replace(/<br\s*\/?>/gi, ' ')   // line breaks → space
    .replace(/`([^`]+)`/g, '$1')    // strip backticks
    .replace(/\*\*([^*]+)\*\*/g, '$1')
    .trim()

  const noteMatch = chunk.match(/<!--\s*([\s\S]*?)\s*-->/)
  const note = noteMatch ? noteMatch[1].trim() : ''

  slides.push({ num: i + 1, title, note })
}

const totalSec = slides.reduce((acc, s) => {
  const m = s.note.match(/\(~(\d+)s/)
  return acc + (m ? parseInt(m[1], 10) : 0)
}, 0)
const mins = Math.floor(totalSec / 60)
const secs = totalSec % 60

// Render each note: preserve line breaks, bold "→ Next:" tail, italicize
// the time hint, escape HTML.
const escapeHtml = s => s
  .replace(/&/g, '&amp;')
  .replace(/</g, '&lt;')
  .replace(/>/g, '&gt;')

const renderNote = (n) => {
  let html = escapeHtml(n)
  // Time hint: "(~45s)" or "(~45s — ...)"
  html = html.replace(/^\(~([^)]+)\)/, '<span class="time">~$1</span>')
  // Inline code-ish: backticks (we stripped from title but notes may keep)
  html = html.replace(/`([^`]+)`/g, '<code>$1</code>')
  // Transition line
  html = html.replace(/(→ Next:[^\n]*)/g, '<div class="next">$1</div>')
  // Bold
  html = html.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
  // Line breaks → <br>
  html = html.replace(/\n/g, '<br>')
  return html
}

const cards = slides.map(s => `
  <article class="slide" id="s${s.num}">
    <header>
      <span class="num">${String(s.num).padStart(2, '0')}</span>
      <h2>${escapeHtml(s.title)}</h2>
    </header>
    ${s.note ? `<div class="note">${renderNote(s.note)}</div>` : '<div class="note empty">— no note —</div>'}
  </article>
`).join('')

const html = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Speaker notes · Streaming MySQL Changes to ClickHouse</title>
<style>
:root {
  color-scheme: light dark;
  --bg: #FAFAF7;
  --bg-card: #FFFFFF;
  --text: #0D1117;
  --text-secondary: #1F2937;
  --muted: #6B7280;
  --border: #E5E7EB;
  --accent: #0B6FD1;
  --time: #B57500;
  --code-bg: #F3F4F6;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0D1117;
    --bg-card: #161B22;
    --text: #F0F6FC;
    --text-secondary: #C9D1D9;
    --muted: #8B949E;
    --border: #30363D;
    --accent: #1F8FFF;
    --time: #FFB020;
    --code-bg: #161B22;
  }
}
* { box-sizing: border-box; }
body {
  margin: 0;
  font-family: 'Space Grotesk', -apple-system, system-ui, sans-serif;
  background: var(--bg);
  color: var(--text);
  line-height: 1.5;
  -webkit-font-smoothing: antialiased;
}
.page {
  max-width: 760px;
  margin: 0 auto;
  padding: 28px 20px 80px;
}
.top {
  border-bottom: 1px solid var(--border);
  padding-bottom: 18px;
  margin-bottom: 24px;
}
.top h1 {
  font-size: 1.4rem;
  margin: 0 0 4px 0;
  letter-spacing: -0.02em;
}
.top .sub {
  font-size: 0.9rem;
  color: var(--muted);
}
.top .meta {
  font-size: 0.8rem;
  color: var(--muted);
  margin-top: 10px;
  display: flex;
  flex-wrap: wrap;
  gap: 8px 14px;
}
.top .meta b {
  color: var(--text-secondary);
  font-weight: 600;
}
.slide {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 18px 20px;
  margin-bottom: 16px;
  scroll-margin-top: 16px;
}
.slide header {
  display: flex;
  align-items: baseline;
  gap: 12px;
  margin-bottom: 10px;
  border-bottom: 1px dashed var(--border);
  padding-bottom: 10px;
}
.slide .num {
  font-variant-numeric: tabular-nums;
  font-weight: 700;
  color: var(--accent);
  font-size: 0.85rem;
  letter-spacing: 0.04em;
}
.slide h2 {
  margin: 0;
  font-size: 1.05rem;
  letter-spacing: -0.01em;
  font-weight: 700;
  color: var(--text);
}
.note {
  font-size: 0.98rem;
  color: var(--text-secondary);
}
.note.empty {
  color: var(--muted);
  font-style: italic;
}
.note .time {
  display: inline-block;
  font-size: 0.78rem;
  font-weight: 700;
  color: var(--time);
  letter-spacing: 0.08em;
  background: color-mix(in srgb, var(--time) 14%, transparent);
  padding: 2px 8px;
  border-radius: 4px;
  margin-right: 8px;
  vertical-align: 0.06em;
}
.note .next {
  display: inline-block;
  margin-top: 8px;
  font-weight: 600;
  color: var(--accent);
  font-size: 0.9rem;
}
.note code {
  background: var(--code-bg);
  border-radius: 4px;
  padding: 1px 6px;
  font-family: 'JetBrains Mono', ui-monospace, monospace;
  font-size: 0.88em;
}
.toc {
  margin-top: 32px;
  padding-top: 20px;
  border-top: 1px solid var(--border);
  font-size: 0.85rem;
  color: var(--muted);
}
.toc a {
  color: var(--text-secondary);
  text-decoration: none;
  display: inline-block;
  padding: 3px 8px;
  margin: 2px;
  border-radius: 4px;
  background: var(--bg-card);
  border: 1px solid var(--border);
  font-variant-numeric: tabular-nums;
}
.toc a:hover {
  border-color: var(--accent);
  color: var(--accent);
}
</style>
</head>
<body>
<div class="page">
  <div class="top">
    <h1>Speaker notes</h1>
    <div class="sub">Streaming MySQL Changes to ClickHouse · Javier Zon · Percona Live 26</div>
    <div class="meta">
      <span><b>${slides.length}</b> slides</span>
      <span>Content: <b>~${mins}:${String(secs).padStart(2, '0')}</b></span>
      <span>Slot: <b>30 min</b> · Q&amp;A: <b>~${30 - mins} min</b></span>
    </div>
  </div>
  ${cards}
  <div class="toc">
    Jump to:
    ${slides.map(s => `<a href="#s${s.num}">${s.num}</a>`).join('')}
  </div>
</div>
</body>
</html>
`

fs.writeFileSync(outFile, html)
console.log(`✓ Wrote ${path.relative(repoRoot, outFile)} — ${slides.length} slides, ~${mins}:${String(secs).padStart(2, '0')} total`)
