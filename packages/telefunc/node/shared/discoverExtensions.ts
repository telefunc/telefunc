export { getExtensionImports }

import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { createRequire } from 'node:module'

type PackageJson = {
  dependencies?: Record<string, string>
  devDependencies?: Record<string, string>
  /** Each entry is the full bare import specifier, e.g. `"@telefunc/rxjs/server"`. */
  telefunc?: { server?: string; client?: string }
}

/** Return one `import '<specifier>';` statement per `@telefunc/*` extension declared for the given side. */
function getExtensionImports(root: string, side: 'server' | 'client'): string[] {
  const lines: string[] = []
  for (const { telefunc } of iterateExtensions(root)) {
    const specifier = telefunc[side]
    if (specifier !== undefined) lines.push(`import '${specifier}';`)
  }
  return lines
}

/**
 * Yield each `@telefunc/*` extension package declared in the project's package.json
 * along with its `"telefunc"` manifest field.
 *
 * Works in monorepos because it uses Node's module resolution (createRequire)
 * rather than scanning node_modules directories.
 */
function* iterateExtensions(root: string): Generator<{ telefunc: NonNullable<PackageJson['telefunc']> }> {
  const pkgJsonPath = join(root, 'package.json')
  const pkgJson = readJson<PackageJson>(pkgJsonPath)
  if (!pkgJson) return

  const require = createRequire(pkgJsonPath)

  for (const name of Object.keys({ ...pkgJson.dependencies, ...pkgJson.devDependencies })) {
    if (!name.startsWith('@telefunc/')) continue

    const extPkgJsonPath = tryResolve(require, `${name}/package.json`)
    if (!extPkgJsonPath) continue

    const telefunc = readJson<PackageJson>(extPkgJsonPath)?.telefunc
    if (!telefunc) continue

    yield { telefunc }
  }
}

function tryResolve(require: ReturnType<typeof createRequire>, request: string): string | null {
  try {
    return require.resolve(request)
  } catch {
    return null
  }
}

function readJson<T>(path: string): T | null {
  try {
    return JSON.parse(readFileSync(path, 'utf-8')) as T
  } catch {
    return null
  }
}
