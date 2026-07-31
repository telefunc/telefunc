import assert from 'node:assert/strict'
import { readFile, readdir } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const root = path.dirname(fileURLToPath(import.meta.url))
const server = path.join(root, 'dist', 'server')
const files = (await walk(server))
  .map((file) => path.relative(server, file).replaceAll(path.sep, '/'))
  .filter((file) => file.endsWith('.js') || file.endsWith('.mjs'))
  .filter((file) => !file.startsWith('.vite/') && !file.startsWith('.wrangler/'))
  .sort()

const allowedExternalImports = new Set(['cloudflare:workers', 'node:async_hooks'])
const importedExternals = new Set()
for (const relative of files) {
  const source = await readFile(path.join(server, relative), 'utf8')
  assert(!/(?:^|[^\w])ioredis(?:[^\w]|$)|@telefunc\/redis/i.test(source), `${relative} contains Redis code`)
  assert(
    !/Miniflare|RECOVERY_KV|telefuncRoom(?:Seed|Drop)|ForTest|backend\/conformance/.test(source),
    `${relative} contains test-only Cloudflare controls`,
  )
  for (const specifier of moduleSpecifiers(source)) {
    if (specifier.startsWith('.')) continue
    importedExternals.add(specifier)
    assert(allowedExternalImports.has(specifier), `${relative} imports unexpected external module '${specifier}'`)
  }
}
assert.deepEqual([...importedExternals].sort(), [...allowedExternalImports].sort())

const wrangler = JSON.parse(await readFile(path.join(server, 'wrangler.json'), 'utf8'))
// spellcheck-ignore
assert(wrangler.compatibility_flags.includes('nodejs_als'), 'node:async_hooks requires the nodejs_als flag')
assert.deepEqual(wrangler.durable_objects.bindings.map(({ name }) => name).sort(), [
  'TO_DO_LIST_DURABLE_OBJECTS',
  'TelefuncDurableObject',
  'TelefuncRoomDurableObject',
])
assert(
  wrangler.migrations.some(
    ({ tag, new_sqlite_classes: classes }) => tag === 'v2' && classes?.includes('TelefuncRoomDurableObject'),
  ),
  'missing v2 SQLite Room Durable Object migration',
)

async function walk(directory) {
  const entries = await readdir(directory, { withFileTypes: true })
  return (
    await Promise.all(
      entries.map((entry) => {
        const child = path.join(directory, entry.name)
        return entry.isDirectory() ? walk(child) : [child]
      }),
    )
  ).flat()
}

function moduleSpecifiers(source) {
  const specifiers = []
  const patterns = [
    /(?:^|\n)\s*(?:import|export)\s+(?:[^'"\n]*?\s+from\s+)?['"]([^'"]+)['"]/g,
    /(?<![\w$])import\s*\(\s*['"]([^'"]+)['"]\s*\)/g,
  ]
  for (const pattern of patterns) {
    for (const match of source.matchAll(pattern)) specifiers.push(match[1])
  }
  return specifiers
}
