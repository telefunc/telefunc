export { generateShield }
export { logResult }

// For ./generateShield.spec.ts
export { testGenerateShield }
export { testGenerateShieldsForFiles }

import { Project, SourceFile, getCompilerOptionsFromTsConfig } from 'ts-morph'
import { assert, assertUsage, assertWarning } from '../../../../utils/assert.js'
import { assertModuleScope } from '../../../../utils/assertModuleScope.js'
import { getRandomId } from '../../../../utils/getRandomId.js'
import { objectAssign } from '../../../../utils/objectAssign.js'
import { unique } from '../../../../utils/unique.js'
import { type ExportList, getExportList } from '../getExportList.js'
import { findTsConfig } from './findTsConfig.js'
import fs from 'node:fs'
import path from 'node:path'
import { createHash } from 'node:crypto'
import pc from '@brillout/picocolors'
import { fileURLToPath } from 'node:url'
const __dirname_ = path.dirname(fileURLToPath(import.meta.url))

type GeneratedShield = {
  telefuncFilePath: string
  telefunctionName: string
  failed: boolean
  project: Project & { tsConfigFilePath: null | string }
}

assertModuleScope('generateShield/generateShield.ts')
const generatedShields: GeneratedShield[] = []
let resultAlreadyLogged = false
const projects: Record<string, Project> = {}

// Cache populated by both the batch path and the per-file fallback.
// Keyed by absolute module id; `code` is the source the cached shield
// was generated from, gating staleness so two files with identical
// source but different import neighborhoods stay separate (different
// id) and HMR edits regenerate (same id, different code).
const shieldCache: Map<string, { code: string; shield: string }> = new Map()

// One in-flight batch per tsconfig. Concurrent transform() calls collapse onto it.
const batchPromises: Map<string, Promise<void>> = new Map()

async function generateShield(
  telefuncFileCode: string,
  telefuncFilePath: string,
  appRootDir: string,
  exportList: ExportList,
): Promise<string> {
  const cached = shieldCache.get(telefuncFilePath)
  if (cached?.code === telefuncFileCode) return cached.shield

  const tsConfigFilePath = findTsConfig(telefuncFilePath, appRootDir)
  if (tsConfigFilePath) {
    await getOrStartBatch(tsConfigFilePath)
    const fresh = shieldCache.get(telefuncFilePath)
    if (fresh?.code === telefuncFileCode) return fresh.shield
    if (process.env.TELEFUNC_SHIELD_DEBUG) {
      // Common cause: an upstream plugin transformed the file before
      // telefunc, so the batch's on-disk read doesn't match this code.
      process.stderr.write(
        `[telefunc] cache miss after batch: file=${telefuncFilePath} — falling back to per-file\n`,
      )
    }
  }

  // Per-file fallback: no tsconfig, batch skipped this file (parse
  // error in its export list), or content changed after the batch ran.
  const shieldCode = generateShieldPerFile(telefuncFileCode, telefuncFilePath, appRootDir, exportList)
  shieldCache.set(telefuncFilePath, { code: telefuncFileCode, shield: shieldCode })
  return shieldCode
}

function getOrStartBatch(tsConfigFilePath: string): Promise<void> {
  let p = batchPromises.get(tsConfigFilePath)
  if (!p) {
    p = runBatchForTsConfig(tsConfigFilePath)
    batchPromises.set(tsConfigFilePath, p)
  }
  return p
}

function generateShieldPerFile(
  telefuncFileCode: string,
  telefuncFilePath: string,
  appRootDir: string,
  exportList: ExportList,
): string {
  const project = ensureProjectHasFile(telefuncFilePath, telefuncFileCode, appRootDir)
  const shields = generateShieldsForFiles(project, [
    { filePath: telefuncFilePath, code: telefuncFileCode, exportList },
  ])
  const shield = shields.get(telefuncFilePath)
  assert(shield !== undefined)
  return shield
}

function ensureProject(tsConfigFilePath: string | null): Project {
  const key = tsConfigFilePath ?? '__no_tsconfig'
  if (projects[key]) return projects[key]!

  // When shield() generation fails, avoid showing unrelated errors in TypeScript diagnostics
  const tsConfigAddendum = { skipLibCheck: true }
  let project: Project
  if (!tsConfigFilePath) {
    project = projects[key] = new Project({
      compilerOptions: {
        ...tsConfigAddendum,
        // See assertUsage() comment
        strict: true,
      },
    })
  } else {
    project = projects[key] = new Project({
      tsConfigFilePath,
      // Add all project files, which is needed for picking up the Telefunc.Context value
      //  - What `Telefunc.Context` is, is explained at https://telefunc.com/getContext#typescript
      skipAddingFilesFromTsConfig: false,
      compilerOptions: {
        ...tsConfigAddendum,
      },
    })

    const compilerOptionsUser = getCompilerOptionsFromTsConfig(tsConfigFilePath)
    // We need `compilerOptions.strict` to avoid `TS2589: Type instantiation is excessively deep and possibly infinite.`
    assertUsage(
      compilerOptionsUser.options.strict === true,
      `Set \`compilerOptions.strict\` to \`true\` in ${tsConfigFilePath} (needed for shield() generation)`,
    )
  }

  const compilerOptionsResolved = project.compilerOptions.get()
  assert(compilerOptionsResolved.strict === true)
  assert(compilerOptionsResolved.skipLibCheck === true)

  // This source file is used for evaluating the template literal types' values
  const typeToShieldFilePath = path.join(getFilesystemRoot(), '__telefunc_TypeToShield.ts')
  project.createSourceFile(typeToShieldFilePath, getTypeToShieldSrc())

  return project
}

function ensureProjectHasFile(
  telefuncFilePath: string,
  telefuncFileCode: string,
  appRootDir: string,
  isTest?: true,
): Project & { tsConfigFilePath: null | string } {
  const tsConfigFilePath = isTest ? null : findTsConfig(telefuncFilePath, appRootDir)

  const project = ensureProject(tsConfigFilePath)
  objectAssign(project, { tsConfigFilePath })

  if (!tsConfigFilePath && !project.getSourceFile(telefuncFilePath)) {
    project.createSourceFile(telefuncFilePath, telefuncFileCode, { overwrite: true })
  }

  const telefuncFileSource = project.getSourceFile(telefuncFilePath)
  assertTelefuncFilesSource(telefuncFileSource, { project, telefuncFilePath, tsConfigFilePath, appRootDir })
  // Skip the write when the in-project text already matches:
  // `replaceWithText` invalidates the Program and forces a fresh TypeChecker.
  if (telefuncFileSource.getFullText() !== telefuncFileCode) {
    telefuncFileSource.replaceWithText(telefuncFileCode)
  }

  return project
}

type ShieldFileInput = { filePath: string; code: string; exportList: ExportList }

// Pure-ish primitive: given a project with the listed files already loaded
// and an explicit (filePath, code, exportList) for each, generate shield
// code for every file in one pass that shares a single TypeChecker.
//
// Both the per-tsconfig batch and the per-file fallback flow through here;
// callers decide *which* files to process and pass a precomputed exportList
// so parse-error handling is the caller's policy choice.
function generateShieldsForFiles(
  project: Project & { tsConfigFilePath: null | string },
  files: ShieldFileInput[],
): Map<string, string> {
  // Use a unique aggregate filename per call so concurrent invocations on
  // the same project (e.g. parallel per-file fallbacks) don't collide.
  const aggFilePath = path.join(getFilesystemRoot(), `__telefunc_shieldGen_${getRandomId()}.ts`)
  const aggSource = project.createSourceFile(aggFilePath, undefined, { overwrite: true })

  const typeToShieldFilePath = path.join(getFilesystemRoot(), '__telefunc_TypeToShield.ts')
  aggSource.addImportDeclaration({
    moduleSpecifier: getImportPath(aggFilePath, typeToShieldFilePath),
    namedImports: ['TypeToShield'],
  })

  type Plan = { filePath: string; exportList: ExportList; fileTag: string }
  const plans: Plan[] = []

  for (const { filePath, exportList } of files) {
    if (exportList.length === 0) continue
    // Tag is derived from the file path so identical export names
    // across files (very common: `useFoo`, `getX`) don't collide.
    const fileTag = makeFileTag(filePath)
    aggSource.addImportDeclaration({
      moduleSpecifier: getImportPath(aggFilePath, filePath).replace(/\.ts$/, ''),
      namedImports: exportList.map((e) => ({
        name: e.exportName,
        alias: `${e.exportName}__${fileTag}`,
      })),
    })
    for (const e of exportList) {
      const ta = aggSource.addTypeAlias({
        name: `${getShieldName(e.exportName)}__${fileTag}`,
        type: `TypeToShield<typeof ${e.exportName}__${fileTag}>`,
      })
      // Suppress TS6196 "is declared but never used" — see issue 229.
      aggSource.insertText(
        ta.getStart(),
        '// @ts-ignore Used internally by Telefunc at compile time (not runtime)\n',
      )
    }
    plans.push({ filePath, exportList, fileTag })
  }

  // One walk for the whole batch. After this, no AST mutations happen
  // until the loop below finishes — so all the type reads share the
  // same TypeChecker.
  project.resolveSourceFileDependencies()
  assert(project.compilerOptions.get().strict === true)

  const planByFilePath = new Map(plans.map((p) => [p.filePath, p]))
  const result = new Map<string, string>()

  for (const { filePath, exportList } of files) {
    let shieldCode = [
      'import { shield as __telefunc_shield } from "telefunc";',
      'const __telefunc_t = __telefunc_shield.type;',
    ].join('\n')

    const plan = planByFilePath.get(filePath)
    if (plan) {
      for (const exportedFunction of exportList) {
        const aliasName = `${getShieldName(exportedFunction.exportName)}__${plan.fileTag}`
        const ta = aggSource.getTypeAlias(aliasName)
        assert(ta, `Failed to get type alias \`${aliasName}\`.`)

        const shieldStrType = ta.getType()
        const shieldStr = shieldStrType.getLiteralValue()
        assert(shieldStr === undefined || typeof shieldStr === 'string')

        if (shieldStr === 'NON_FUNCTION_EXPORT') continue

        const failed = shieldStr === undefined
        generatedShields.push({
          project,
          telefuncFilePath: filePath,
          telefunctionName: exportedFunction.exportName,
          failed,
        })

        if (failed) continue

        shieldCode += '\n'
        shieldCode += `__telefunc_shield(${
          exportedFunction.localName || exportedFunction.exportName
        }, ${shieldStr}, { __autoGenerated: true });`
      }
    }
    shieldCode += '\n'
    result.set(filePath, shieldCode)
  }

  aggSource.delete()
  return result
}

// Per-tsconfig batch policy: discover every `.telefunc.ts` already loaded
// into the project (tsconfig's `include` is the authority), pre-validate
// each one (exportList computed here so parse errors fall through to the
// per-file path with the original error), then call the primitive once.
async function runBatchForTsConfig(tsConfigFilePath: string): Promise<void> {
  const debug = process.env.TELEFUNC_SHIELD_DEBUG
  const _t0 = Date.now()
  const project = ensureProject(tsConfigFilePath)
  objectAssign(project, { tsConfigFilePath })

  const telefuncSources = project
    .getSourceFiles()
    .filter((sf) => sf.getFilePath().endsWith('.telefunc.ts'))
  if (telefuncSources.length === 0) return

  const inputs: ShieldFileInput[] = []
  for (const sf of telefuncSources) {
    const filePath = sf.getFilePath()
    const code = sf.getFullText()
    let exportList: ExportList
    try {
      exportList = await getExportList(code)
    } catch {
      // Skip — per-file path will re-raise the original error for the user.
      continue
    }
    inputs.push({ filePath, code, exportList })
  }
  if (inputs.length === 0) return

  if (debug) {
    process.stderr.write(
      `[telefunc] batch start: tsconfig=${tsConfigFilePath} telefuncFiles=${inputs.length}\n`,
    )
  }

  const shields = generateShieldsForFiles(project as Project & { tsConfigFilePath: null | string }, inputs)
  for (const { filePath, code } of inputs) {
    const shield = shields.get(filePath)
    if (shield !== undefined) shieldCache.set(filePath, { code, shield })
  }

  if (debug) {
    process.stderr.write(
      `[telefunc] batch done: tsconfig=${tsConfigFilePath} ` +
        `cached=${shieldCache.size} elapsedMs=${Date.now() - _t0}\n`,
    )
  }
}

function makeFileTag(filePath: string): string {
  return createHash('sha256').update(filePath).digest('hex').slice(0, 8)
}

async function testGenerateShield(telefuncFileCode: string): Promise<string> {
  const telefuncFilePath = `virtual-${getRandomId()}.telefunc.ts`
  const project = ensureProjectHasFile(telefuncFilePath, telefuncFileCode, '/fake-user-root-dir/', true)
  const exportList = await getExportList(telefuncFileCode)
  const shields = generateShieldsForFiles(project, [
    { filePath: telefuncFilePath, code: telefuncFileCode, exportList },
  ])
  const shield = shields.get(telefuncFilePath)
  assert(shield !== undefined)
  return shield
}

// Exercises the multi-file primitive directly. Each input is loaded
// into the (shared) no-tsconfig project under a unique virtual path so
// repeated calls don't collide.
async function testGenerateShieldsForFiles(
  files: { fileName: string; code: string }[],
): Promise<Map<string, string>> {
  const dir = `/test-${getRandomId()}`
  let project: Project & { tsConfigFilePath: null | string } | undefined
  const inputs: ShieldFileInput[] = []
  for (const { fileName, code } of files) {
    const filePath = path.posix.join(dir, fileName)
    project = ensureProjectHasFile(filePath, code, '/fake-user-root-dir/', true)
    inputs.push({ filePath, code, exportList: await getExportList(code) })
  }
  assert(project)
  const shields = generateShieldsForFiles(project, inputs)
  return new Map(inputs.map(({ filePath }, i) => [files[i]!.fileName, shields.get(filePath)!]))
}

function getImportPath(importer: string, importedFile: string) {
  let importPath = path.relative(path.dirname(importer), importedFile)
  importPath = toImport(importPath)
  return importPath
}

function toImport(importPath: string) {
  assert(importPath.endsWith('.ts'))
  importPath = importPath.slice(0, -1 * '.ts'.length)
  if (process.platform === 'win32') {
    importPath = importPath.split('\\').join('/')
  }
  return `./${importPath}`
}

function logResult(appRootDir: string, logSuccessPrefix: string, logIntro: null | string) {
  // `generatedShields` is empty for JavaScript users
  if (generatedShields.length === 0) return
  if (resultAlreadyLogged) {
    assert(generatedShields.length === 0)
    return
  }
  if (logIntro) console.log(logIntro)
  printSuccesses(appRootDir, logSuccessPrefix)
  printFailures(appRootDir)
  resultAlreadyLogged = true
  generatedShields.length = 0
}

function printFailures(appRootDir: string) {
  const failures = generatedShields.filter((s) => s.failed)
  const projects = unique(failures.map((f) => f.project))

  let hasTypeScriptErrors = false
  projects.forEach((project) => {
    const diagnostics = project.getPreEmitDiagnostics()
    if (diagnostics.length > 0) {
      hasTypeScriptErrors = true
      if (project.tsConfigFilePath) {
        console.log(`TypeScript project ${pc.bold(project.tsConfigFilePath)} errors:`)
      } else {
        console.log(`TypeScript errors:`)
      }
      console.log(project.formatDiagnosticsWithColorAndContext(diagnostics))
    }
  })

  assertWarning(
    failures.length === 0,
    [
      'Failed to generate shield() for telefunction',
      failures.length === 1 ? '' : 's',
      ' ',
      formatGeneratedShields(failures, appRootDir),
      '.',
      !hasTypeScriptErrors
        ? ''
        : ' TypeScript errors (printed above) can be problematic for shield() generation. Fix your TypeScript errors and try again.',
      ' See https://telefunc.com/shield#typescript-automatic for more information.',
    ].join(''),
    { onlyOnce: true },
  )
}

function printSuccesses(appRootDir: string, logSuccessPrefix: string) {
  const successes = generatedShields.filter((s) => !s.failed)
  if (successes.length > 0) {
    console.log(
      [
        logSuccessPrefix,
        `shield() generated for the telefunction${generatedShields.length === 1 ? '' : 's'}`,
        formatGeneratedShields(successes, appRootDir),
      ].join(' '),
    )
  }
}

function formatGeneratedShields(generatedShields: GeneratedShield[], appRootDir: string) {
  return formatList(
    generatedShields.map(({ telefunctionName, telefuncFilePath }) => {
      telefuncFilePath = path.relative(appRootDir, telefuncFilePath)
      return `${telefunctionName}() (${telefuncFilePath})`
    }),
  )
}

function formatList(list: string[]): string {
  return new Intl.ListFormat('en').format(list)
}

function getShieldName(telefunctionName: string) {
  return `${telefunctionName}Shield` as const
}

let typeToShieldFileSrc: string | undefined
function getTypeToShieldSrc() {
  if (!typeToShieldFileSrc) {
    try {
      typeToShieldFileSrc = fs.readFileSync(`${__dirname_}/TypeToShield.d.ts`).toString()
    } catch {
      typeToShieldFileSrc = fs.readFileSync(`${__dirname_}/TypeToShield.ts`).toString()
    }
  }
  assert(typeToShieldFileSrc)
  assert(typeToShieldFileSrc.includes('SimpleType'))
  return typeToShieldFileSrc
}

function getFilesystemRoot(): string {
  if (process.platform !== 'win32') {
    return '/'
  }
  const fsRoot = process.cwd().split(path.sep)[0]
  assert(fsRoot)
  return fsRoot
}

function assertTelefuncFilesSource(
  telefuncFileSource: SourceFile | undefined,
  {
    telefuncFilePath,
    project,
    tsConfigFilePath,
    appRootDir,
  }: {
    telefuncFilePath: string
    project: Project
    tsConfigFilePath: string | null
    appRootDir: string
  },
): asserts telefuncFileSource is SourceFile {
  if (telefuncFileSource) {
    return
  }

  const sourceFiles: string[] = project.getSourceFiles().map(
    (sourceFile) =>
      // @ts-expect-error
      sourceFile._compilerNode.fileName,
  )
  if (tsConfigFilePath) {
    const userTsFiles = sourceFiles.filter((filePath) => !filePath.includes('__telefunc_'))
    const msg1 = `The TypeScript configuration ${tsConfigFilePath} doesn't seem to include`
    const msg2 = `Make sure to configure the ${pc.cyan('include')} and ${pc.cyan('exclude')} (or ${pc.cyan(
      'files',
    )}) options of that tsconfig.json` as const
    if (userTsFiles.length === 0) {
      assertUsage(
        false,
        [`${msg1} any file (i.e. it includes 0 files).`, `${msg2} to match at least one file.`].join(' '),
      )
    } else {
      assertUsage(
        false,
        [
          `${msg1} the ${telefuncFilePath} file.`,
          `${msg2} to match the ${telefuncFilePath} file.`,
          `It currently matches the following files:\n${userTsFiles.map((f) => `  ${f}`).join('\n')}`,
        ].join(' '),
      )
    }
  } else {
    const debugInfo = JSON.stringify(
      {
        telefuncFilePath,
        sourceFiles,
        tsConfigFilePath,
        appRootDir,
      },
      null,
      2,
    )
    assert(false, debugInfo)
  }
}
