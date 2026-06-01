export { findTsConfig }

import { ts } from 'ts-morph'
import { assert } from '../../../../utils/assert.js'
import { assertPosixPath } from '../../../../utils/path.js'
import fs from 'node:fs'
import path from 'node:path'

const cache: Record<string, string | null> = {}

// Matches the TypeScript language server algorithm: walk up to find the nearest tsconfig,
// then check project references to find a more specific tsconfig that claims the file.
// https://github.com/telefunc/telefunc/pull/241
// https://github.com/telefunc/telefunc/pull/241#issuecomment-4228361639
function findTsConfig(telefuncFilePath: string, appRootDir: string): string | null {
  assert(fs.existsSync(telefuncFilePath))
  assertPosixPath(telefuncFilePath)
  assertPosixPath(appRootDir)
  assert(telefuncFilePath.startsWith(appRootDir))

  const dir = path.dirname(telefuncFilePath)
  if (dir in cache) return cache[dir]!

  const nearestTsConfig = findNearestTsConfig(telefuncFilePath, appRootDir)
  if (!nearestTsConfig) {
    cache[dir] = null
    return null
  }

  const realFilePath = realpathSync(telefuncFilePath)
  const referencedTsConfig = findOwningReference(nearestTsConfig, realFilePath)
  const result = referencedTsConfig ?? nearestTsConfig
  cache[dir] = result
  return result
}

function findNearestTsConfig(filePath: string, appRootDir: string): string | null {
  let curr = filePath
  do {
    const dir = path.dirname(curr)
    if (dir === curr) return null
    if (!dir.startsWith(appRootDir)) return null
    const tsConfigFilePath = path.join(dir, 'tsconfig.json')
    if (fs.existsSync(tsConfigFilePath)) return tsConfigFilePath
    curr = dir
  } while (true)
}

function findOwningReference(
  tsConfigPath: string,
  realFilePath: string,
  visited: Set<string> = new Set(),
): string | null {
  const realTsConfigPath = realpathSync(tsConfigPath)
  if (visited.has(realTsConfigPath)) return null
  visited.add(realTsConfigPath)

  const config = readTsConfig(realTsConfigPath)
  if (!config) return null
  const references = config.references
  if (!Array.isArray(references) || references.length === 0) return null

  for (const ref of references) {
    const refTsConfigPath = resolveReferencePath(tsConfigPath, ref.path)
    if (!refTsConfigPath) continue

    if (tsConfigIncludesFile(refTsConfigPath, realFilePath)) {
      return refTsConfigPath
    }

    const nested = findOwningReference(refTsConfigPath, realFilePath, visited)
    if (nested) return nested
  }

  return null
}

function realpathSync(filePath: string): string {
  return fs.realpathSync(filePath)
}

function readTsConfig(realTsConfigPath: string): any | null {
  const configFile = ts.readConfigFile(realTsConfigPath, ts.sys.readFile)
  return configFile.error ? null : configFile.config
}

function resolveReferencePath(fromTsConfig: string, refPath: string): string | null {
  const resolved = path.resolve(path.dirname(fromTsConfig), refPath)
  try {
    if (fs.statSync(resolved).isDirectory()) {
      return path.join(resolved, 'tsconfig.json')
    }
    return resolved
  } catch (err: any) {
    if (err?.code === 'ENOENT' || err?.code === 'ENOTDIR') return null
    throw err
  }
}

function tsConfigIncludesFile(tsConfigPath: string, realFilePath: string): boolean {
  const realTsConfigPath = realpathSync(tsConfigPath)
  const config = readTsConfig(realTsConfigPath)
  if (!config) return false

  const configDir = path.dirname(realTsConfigPath)
  const parsed = ts.parseJsonConfigFileContent(config, ts.sys, configDir)
  const fileNames = new Set(parsed.fileNames.map((f) => realpathSync(path.resolve(f))))
  return fileNames.has(realFilePath)
}
