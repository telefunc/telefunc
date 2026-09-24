import { findTsConfig } from './findTsConfig.js'
import { describe, it, expect, afterEach } from 'vitest'
import fs from 'node:fs'
import path from 'node:path'
import os from 'node:os'

function createTempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'telefunc-test-'))
}

function toPosix(p: string) {
  return p.split(path.sep).join('/')
}

function writeJson(filePath: string, content: object) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true })
  fs.writeFileSync(filePath, JSON.stringify(content, null, 2))
}

function writeFile(filePath: string, content: string = '') {
  fs.mkdirSync(path.dirname(filePath), { recursive: true })
  fs.writeFileSync(filePath, content)
}

const tempDirs: string[] = []

function setup() {
  const dir = createTempDir()
  tempDirs.push(dir)
  return toPosix(fs.realpathSync(dir))
}

afterEach(() => {
  for (const dir of tempDirs) {
    fs.rmSync(dir, { recursive: true, force: true })
  }
  tempDirs.length = 0
})

describe('findTsConfig', () => {
  it('finds nearest tsconfig.json by walking up', () => {
    const root = setup()
    writeJson(`${root}/tsconfig.json`, { compilerOptions: { strict: true } })
    writeFile(`${root}/src/hello.telefunc.ts`, 'export function onHello() {}')

    const result = findTsConfig(`${root}/src/hello.telefunc.ts`, `${root}`)
    expect(toPosix(result!)).toBe(`${root}/tsconfig.json`)
  })

  it('returns null when no tsconfig exists', () => {
    const root = setup()
    writeFile(`${root}/src/hello.telefunc.ts`, 'export function onHello() {}')

    const result = findTsConfig(`${root}/src/hello.telefunc.ts`, `${root}`)
    expect(result).toBeNull()
  })

  it('finds tsconfig in a nested package directory', () => {
    const root = setup()
    writeJson(`${root}/tsconfig.json`, { compilerOptions: { strict: true } })
    writeJson(`${root}/packages/app/tsconfig.json`, { compilerOptions: { strict: true } })
    writeFile(`${root}/packages/app/src/hello.telefunc.ts`, 'export function onHello() {}')

    const result = findTsConfig(`${root}/packages/app/src/hello.telefunc.ts`, `${root}`)
    expect(toPosix(result!)).toBe(`${root}/packages/app/tsconfig.json`)
  })

  it('resolves via project references to find owning child tsconfig', () => {
    const root = setup()
    // Root tsconfig with references
    writeJson(`${root}/tsconfig.json`, {
      compilerOptions: { strict: true },
      files: [],
      references: [{ path: './tsconfig.lib.json' }, { path: './tsconfig.app.json' }],
    })
    // tsconfig.lib.json includes src/lib/**
    writeJson(`${root}/tsconfig.lib.json`, {
      compilerOptions: { strict: true },
      include: ['src/lib/**/*.ts'],
    })
    // tsconfig.app.json includes src/app/**
    writeJson(`${root}/tsconfig.app.json`, {
      compilerOptions: { strict: true },
      include: ['src/app/**/*.ts'],
    })
    writeFile(`${root}/src/app/hello.telefunc.ts`, 'export function onHello() {}')

    const result = findTsConfig(`${root}/src/app/hello.telefunc.ts`, `${root}`)
    expect(toPosix(result!)).toBe(`${root}/tsconfig.app.json`)
  })

  it('falls back to nearest tsconfig when no reference claims the file', () => {
    const root = setup()
    writeJson(`${root}/tsconfig.json`, {
      compilerOptions: { strict: true },
      references: [{ path: './tsconfig.lib.json' }],
    })
    writeJson(`${root}/tsconfig.lib.json`, {
      compilerOptions: { strict: true },
      include: ['lib/**/*.ts'],
    })
    // File is NOT under lib/, so tsconfig.lib.json doesn't claim it
    writeFile(`${root}/src/hello.telefunc.ts`, 'export function onHello() {}')

    const result = findTsConfig(`${root}/src/hello.telefunc.ts`, `${root}`)
    expect(toPosix(result!)).toBe(`${root}/tsconfig.json`)
  })

  it('handles references pointing to directories', () => {
    const root = setup()
    writeJson(`${root}/tsconfig.json`, {
      compilerOptions: { strict: true },
      files: [],
      references: [{ path: './packages/core' }],
    })
    writeJson(`${root}/packages/core/tsconfig.json`, {
      compilerOptions: { strict: true },
      include: ['src/**/*.ts'],
    })
    writeFile(`${root}/packages/core/src/hello.telefunc.ts`, 'export function onHello() {}')

    const result = findTsConfig(`${root}/packages/core/src/hello.telefunc.ts`, `${root}`)
    // Tier A finds packages/core/tsconfig.json directly (nearest), so it should return that
    // Tier B would also find it via references, but Tier A's nearest is already correct
    expect(toPosix(result!)).toBe(`${root}/packages/core/tsconfig.json`)
  })

  it('handles nested project references (depth-first)', () => {
    const root = setup()
    writeJson(`${root}/tsconfig.json`, {
      compilerOptions: { strict: true },
      files: [],
      references: [{ path: './packages/core' }],
    })
    writeJson(`${root}/packages/core/tsconfig.json`, {
      compilerOptions: { strict: true },
      files: [],
      references: [{ path: './tsconfig.lib.json' }],
    })
    writeJson(`${root}/packages/core/tsconfig.lib.json`, {
      compilerOptions: { strict: true },
      include: ['src/**/*.ts'],
    })
    writeFile(`${root}/packages/core/src/hello.telefunc.ts`, 'export function onHello() {}')

    const result = findTsConfig(`${root}/packages/core/src/hello.telefunc.ts`, `${root}`)
    // Tier A finds packages/core/tsconfig.json, Tier B follows its reference to tsconfig.lib.json
    expect(toPosix(result!)).toBe(`${root}/packages/core/tsconfig.lib.json`)
  })

  it('survives circular references without infinite loop', () => {
    const root = setup()
    // Two tsconfigs that reference each other — neither claims the file
    writeJson(`${root}/tsconfig.json`, {
      compilerOptions: { strict: true },
      include: ['nope/**/*.ts'],
      references: [{ path: './tsconfig.other.json' }],
    })
    writeJson(`${root}/tsconfig.other.json`, {
      compilerOptions: { strict: true },
      include: ['also-nope/**/*.ts'],
      references: [{ path: './tsconfig.json' }],
    })
    writeFile(`${root}/src/hello.telefunc.ts`, 'export function onHello() {}')

    // Should not hang — no reference claims the file, falls back to nearest tsconfig
    const result = findTsConfig(`${root}/src/hello.telefunc.ts`, `${root}`)
    expect(toPosix(result!)).toBe(`${root}/tsconfig.json`)
  })

  it('does not search above appRootDir boundary', () => {
    const root = setup()
    // tsconfig only exists above the appRootDir
    writeJson(`${root}/tsconfig.json`, { compilerOptions: { strict: true } })
    const subDir = `${root}/sub`
    writeFile(`${subDir}/src/hello.telefunc.ts`, 'export function onHello() {}')

    const result = findTsConfig(`${subDir}/src/hello.telefunc.ts`, subDir)
    expect(result).toBeNull()
  })

  it('first matching reference wins in depth-first order', () => {
    const root = setup()
    writeJson(`${root}/tsconfig.json`, {
      compilerOptions: { strict: true },
      files: [],
      references: [{ path: './tsconfig.first.json' }, { path: './tsconfig.second.json' }],
    })
    // Both claim the same file
    writeJson(`${root}/tsconfig.first.json`, {
      compilerOptions: { strict: true },
      include: ['src/**/*.ts'],
    })
    writeJson(`${root}/tsconfig.second.json`, {
      compilerOptions: { strict: true },
      include: ['src/**/*.ts'],
    })
    writeFile(`${root}/src/hello.telefunc.ts`, 'export function onHello() {}')

    const result = findTsConfig(`${root}/src/hello.telefunc.ts`, `${root}`)
    expect(toPosix(result!)).toBe(`${root}/tsconfig.first.json`)
  })
})
