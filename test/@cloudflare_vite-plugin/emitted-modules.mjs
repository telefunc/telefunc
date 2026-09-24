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
  for (const specifier of moduleSpecifiers(source)) {
    if (specifier.startsWith('.')) continue
    importedExternals.add(specifier)
    assert(allowedExternalImports.has(specifier), `${relative} imports unexpected external module '${specifier}'`)
  }
}
assert.deepEqual([...importedExternals].sort(), [...allowedExternalImports].sort())

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
