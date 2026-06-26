// Static docs quality gate — runs without building or starting a server.
//
// Checks, across docs/pages/**/+Page.mdx and docs/components/**/*.mdx:
//   1. Anchor integrity — every internal `#anchor` link resolves to a real heading.
//   2. Link convention — internal links use <Link>, not bare markdown links.
//
// Run: `node docs/check-docs.mjs` (or `pnpm run docs:lint`).

import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const docsDir = path.dirname(fileURLToPath(import.meta.url))
const pagesDir = path.join(docsDir, 'pages')
const componentsDir = path.join(docsDir, 'components')

// Exact replica of docpress's heading-id algorithm.
// Source: node_modules/@brillout/docpress/dist/utils/determineSectionUrlHash.js
function slugify(title) {
  title = title.toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '')
  const hash = title
    // 一-龥 are Chinese characters (docpress PR #2)
    .split(/[^a-z0-9一-龥]+/)
    .filter(Boolean)
    .join('-')
  return hash === '' ? null : hash
}

// Reduce a markdown heading to the text docpress hashes: drop code-span backticks
// and any inline JSX/HTML tags, keep the visible words.
function headingText(raw) {
  return raw
    .replace(/`/g, '')
    .replace(/<[^>]+>/g, '')
    .trim()
}

function walk(dir, match) {
  const out = []
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name)
    if (entry.isDirectory()) out.push(...walk(full, match))
    else if (match(entry.name)) out.push(full)
  }
  return out
}

const files = walk(pagesDir, (name) => name === '+Page.mdx')

// url ("/channel", "/warning/non-function-export") -> { slugs, file, src }
const pages = {}
for (const file of files) {
  const url = '/' + path.relative(pagesDir, path.dirname(file)).split(path.sep).join('/')
  const src = fs.readFileSync(file, 'utf8')
  const slugs = new Set()
  for (const line of src.split('\n')) {
    const m = line.match(/^#{1,6}\s+(.*)/)
    if (m) {
      // Honor docpress's custom-anchor syntax `## Title {#custom-id}`: when present,
      // the id (not the slugified title) determines the heading's hash.
      // Source: node_modules/@brillout/docpress/dist/parsePageSections.js
      const customAnchor = /\{#([^}]+)\}/.exec(m[1])?.[1]
      const s = customAnchor ? slugify(customAnchor) : slugify(headingText(m[1]))
      if (s) slugs.add(s)
    }
  }
  pages[url] = { slugs, file, src }
}

const errors = []

// Reusable components (docs/components/**/*.mdx) get embedded into pages, so the same
// link convention applies. They have no URL of their own, so page-relative `#anchor`
// links can't be resolved and are skipped — only explicit cross-page anchors are checked.
const components = walk(componentsDir, (name) => name.endsWith('.mdx')).map((file) => ({
  url: null,
  rel: path.relative(docsDir, file),
  src: fs.readFileSync(file, 'utf8'),
}))

const linkSources = [
  ...Object.entries(pages).map(([url, page]) => ({
    url,
    rel: path.relative(pagesDir, page.file),
    src: page.src,
  })),
  ...components,
]

// Strip fenced code blocks and code spans so links inside examples aren't checked.
function stripCode(src) {
  return src.replace(/```[\s\S]*?```/g, '').replace(/`[^`]*`/g, '')
}

for (const { url, rel, src } of linkSources) {
  const prose = stripCode(src)

  const hrefs = [...src.matchAll(/href="([^"]+)"/g)].map((m) => m[1])
  const mdLinks = [...prose.matchAll(/\]\((\/[^)\s]*|#[^)\s]*)\)/g)].map((m) => m[1])

  // Anchor integrity: every internal `#anchor` link resolves to a real heading.
  for (const href of [...hrefs, ...mdLinks]) {
    if (!href.includes('#')) continue
    if (/^https?:/.test(href)) continue
    const [rawPath, anchor] = href.split('#')
    if (!anchor) continue
    // A page-relative anchor in a URL-less component can't be resolved — skip it.
    if (rawPath === '' && url === null) continue
    const targetUrl = rawPath === '' ? url : rawPath.replace(/\/$/, '')
    const target = pages[targetUrl]
    if (!target) {
      errors.push(`${rel}: link to unknown page "${href}" (no ${targetUrl}/+Page.mdx)`)
    } else if (!target.slugs.has(anchor)) {
      errors.push(`${rel}: broken anchor "${href}" — no heading "#${anchor}" on ${targetUrl}`)
    }
  }

  // Link convention: internal links use <Link>, not bare markdown links.
  for (const m of mdLinks) {
    errors.push(`${rel}: bare markdown internal link "](${m})" — use <Link href="${m}" /> instead`)
  }
}

const linkCount = linkSources.reduce((n, s) => n + [...s.src.matchAll(/href="[^"]*#/g)].length, 0)

if (errors.length) {
  console.error(`\n✗ docs quality gate: ${errors.length} issue(s)\n`)
  for (const e of errors.sort()) console.error('  ' + e)
  console.error('')
  process.exit(1)
}
console.log(
  `✓ docs quality gate: ${files.length} pages, ${components.length} components, ${linkCount} internal anchor links — all resolve.`,
)
