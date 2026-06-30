// Static docs quality gate — runs without building or starting a server.
//
// Checks, across docs/pages/**/+Page.mdx and docs/components/**/*.mdx:
//   1. Anchor integrity — every internal `#anchor` link resolves to a real heading.
//   2. Page integrity — every internal link points to a real page.
//   3. Link convention — internal links use <Link>, not bare markdown links and not
//      absolute `https://telefunc.com/...` URLs (telefunc.com is the docs site itself,
//      so such a link is an internal link in disguise — it must be a relative <Link>).
//
// Package & repo READMEs (README.md, packages/*/README.md) are plain Markdown rendered
// on npm/GitHub, so they can't use <Link> and legitimately link to the docs via absolute
// telefunc.com URLs. Those absolute links are still resolved here so a deep link can't
// silently rot (e.g. a link to /scaling when the page lives at /stream/scale).
//
// Run: `node docs/check-docs.mjs` (run in CI by .github/workflows/ci.yml).

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
  kind: 'mdx',
}))

// Package & repo READMEs are plain Markdown (npm/GitHub), so the <Link> convention can't
// apply — but their absolute telefunc.com links are still resolved (page + anchor) so a
// deep link can't silently break. Listed explicitly to stay out of node_modules.
const repoDir = path.join(docsDir, '..')
const pkgsDir = path.join(repoDir, 'packages')
const readmeFiles = [
  path.join(repoDir, 'README.md'),
  path.join(docsDir, 'README.md'),
  ...(fs.existsSync(pkgsDir)
    ? fs
        .readdirSync(pkgsDir, { withFileTypes: true })
        .filter((d) => d.isDirectory())
        .map((d) => path.join(pkgsDir, d.name, 'README.md'))
    : []),
].filter((f) => fs.existsSync(f))
const readmes = readmeFiles.map((file) => ({
  url: null,
  rel: path.relative(repoDir, file),
  src: fs.readFileSync(file, 'utf8'),
  kind: 'markdown',
}))

const linkSources = [
  ...Object.entries(pages).map(([url, page]) => ({
    url,
    rel: path.relative(pagesDir, page.file),
    src: page.src,
    kind: 'mdx',
  })),
  ...components,
  ...readmes,
]

// telefunc.com is the docs site itself, so an absolute link to it is an internal link in
// disguise. Recognize the common spellings of the origin (with/without www, http/https).
const SELF_ORIGIN = /^https?:\/\/(www\.)?telefunc\.com/i

// Reduce a link target to its docs-relative form ("/stream/scale#x", "#x", "/") or null
// when it points elsewhere (other domains, mailto, etc.).
function toInternal(target) {
  if (SELF_ORIGIN.test(target)) return target.replace(SELF_ORIGIN, '') || '/'
  if (target.startsWith('/') || target.startsWith('#')) return target
  return null
}

// Strip fenced code blocks and code spans so links inside examples aren't checked.
function stripCode(src) {
  return src.replace(/```[\s\S]*?```/g, '').replace(/`[^`]*`/g, '')
}

for (const { url, rel, src, kind } of linkSources) {
  const prose = stripCode(src)

  // Link targets: JSX/HTML href="..." plus every markdown ](target) (classified via toInternal).
  const hrefs = [...src.matchAll(/href="([^"]+)"/g)].map((m) => m[1])
  const mdLinks = [...prose.matchAll(/\]\(([^)\s]+)\)/g)].map((m) => m[1])

  // Page + anchor integrity. Relative links keep their historical scope (resolved only when
  // they carry an `#anchor`, since a bare `/page` may point at a redirect). Absolute
  // telefunc.com links are always resolved, page existence included.
  for (const link of [...hrefs, ...mdLinks]) {
    const internal = toInternal(link)
    if (internal === null) continue // external link — not ours to check
    const isSelf = SELF_ORIGIN.test(link)
    // Relative ("/page", "#anchor") links are docs-relative only in MDX; in plain-Markdown
    // READMEs they're repo-relative (GitHub), so there only absolute telefunc.com links are ours.
    if (!isSelf && kind !== 'mdx') continue
    const [rawPath, anchor] = internal.split('#')
    if (!isSelf && !anchor) continue
    // A page-relative anchor ("#x") in a URL-less source (component / README) can't resolve.
    if (rawPath === '' && url === null) continue
    const targetUrl = rawPath === '' ? url : rawPath.replace(/\/$/, '')
    if (targetUrl === '/' || targetUrl === '') continue // docs home — nothing to resolve
    const page = pages[targetUrl]
    if (!page) {
      errors.push(`${rel}: link to unknown page "${link}" (no ${targetUrl}/+Page.mdx)`)
    } else if (anchor && !page.slugs.has(anchor)) {
      errors.push(`${rel}: broken anchor "${link}" — no heading "#${anchor}" on ${targetUrl}`)
    }
  }

  // Link convention (MDX only — READMEs are plain Markdown and can't use <Link>): internal
  // links must use <Link>, never a bare markdown link nor an absolute telefunc.com URL.
  if (kind === 'mdx') {
    for (const m of mdLinks) {
      const internal = toInternal(m)
      if (internal === null) continue
      errors.push(`${rel}: bare markdown internal link "](${m})" — use <Link href="${internal}" /> instead`)
    }
    for (const href of hrefs) {
      if (!SELF_ORIGIN.test(href)) continue
      const internal = href.replace(SELF_ORIGIN, '') || '/'
      errors.push(`${rel}: absolute internal link href="${href}" — use a relative href="${internal}" instead`)
    }
  }
}

if (errors.length) {
  console.error(`\n✗ docs quality gate: ${errors.length} issue(s)\n`)
  for (const e of errors.sort()) console.error('  ' + e)
  console.error('')
  process.exit(1)
}
console.log(
  `✓ docs quality gate: ${files.length} pages, ${components.length} components, ${readmes.length} READMEs — ` +
    `internal links (anchors, pages, and absolute telefunc.com URLs) all resolve.`,
)
