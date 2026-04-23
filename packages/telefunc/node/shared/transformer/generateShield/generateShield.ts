export { generateShield }
export { logResult }

// For ./generateShield.spec.ts
export { testGenerateShield }

import { Project, SourceFile, Type, TypeAliasDeclaration, getCompilerOptionsFromTsConfig } from 'ts-morph'
import { toPathKey } from '../../../../utils/pathKey.js'
import { assert, assertUsage, assertWarning } from '../../../../utils/assert.js'
import { assertModuleScope } from '../../../../utils/assertModuleScope.js'
import { getRandomId } from '../../../../utils/getRandomId.js'
import { objectAssign } from '../../../../utils/objectAssign.js'
import { assertPosixPath } from '../../../../utils/path.js'
import { unique } from '../../../../utils/unique.js'
import { type ExportList, getExportList } from '../getExportList.js'
import fs from 'node:fs'
import path from 'node:path'
import pc from '@brillout/picocolors'
import { fileURLToPath } from 'node:url'
const __dirname_ = path.dirname(fileURLToPath(import.meta.url))

/** The three code outputs shield generation emits per telefunction:
 *  - `Main`   — `__telefunc_shield(fn, [...verifiers])` — arg-shape validator tuple.
 *  - `Return` — `__applyReturnShields(fn, { ... })` — data-flow shields for the return value.
 *  - `Args`   — `__applyArgumentShields(fn, { ... })` — data-flow shields for the arguments. */
const enum ShieldKind {
  Main = 'main',
  Return = 'return',
  Args = 'args',
}

/** One record per (telefunction, ShieldKind). `Main` is always logged; `Return`/`Args` only when
 *  the telefunction has corresponding sub-shield sites. `failed=true` means the emission couldn't
 *  be produced because a type expression didn't resolve to a literal. */
type GeneratedShield = {
  kind: ShieldKind
  telefuncFilePath: string
  telefunctionName: string
  failed: boolean
  project: Project & { tsConfigFilePath: null | string }
}

assertModuleScope('generateShield/generateShield.ts')
const generatedShields: GeneratedShield[] = []
let resultAlreadyLogged = false
const projects: Record<string, Project> = {}

function generateShield(
  telefuncFileCode: string,
  telefuncFilePath: string,
  appRootDir: string,
  exportList: ExportList,
): string {
  const { project, shieldGenSource } = getProject(telefuncFilePath, telefuncFileCode, appRootDir)
  const shieldCode = generateShieldCode({
    project,
    shieldGenSource,
    telefuncFilePath,
    exportList,
  })
  return shieldCode
}

function getProject(telefuncFilePath: string, telefuncFileCode: string, appRootDir: string, isTest?: true) {
  const tsConfigFilePath = isTest ? null : findTsConfig(telefuncFilePath, appRootDir)
  const key = tsConfigFilePath ?? '__no_tsconfig'
  const typeToShieldFilePath = path.join(getFilesystemRoot(), '__telefunc_TypeToShield.ts')
  // When shield() generation fails, avoid showing unrelated errors in TypeScript diagnostics
  const tsConfigAddendum = { skipLibCheck: true }

  if (!projects[key]) {
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
    project.createSourceFile(typeToShieldFilePath, getTypeToShieldSrc())
  }
  const project = projects[key]!
  objectAssign(project, { tsConfigFilePath })

  if (!tsConfigFilePath) {
    assert(!project.getSourceFile(telefuncFilePath))
    project.createSourceFile(
      telefuncFilePath,
      telefuncFileCode,
      // We need `overwrite` because `telefuncFilePath` already exists on the filesystem
      { overwrite: true },
    )
  }

  const shieldGenFilePath = path.join(
    path.dirname(telefuncFilePath),
    `__telefunc_shieldGen_${path.basename(telefuncFilePath)}`,
  )
  const shieldGenSource = project.createSourceFile(shieldGenFilePath, undefined, { overwrite: true })
  shieldGenSource.addImportDeclaration({
    moduleSpecifier: getImportPath(shieldGenFilePath, typeToShieldFilePath),
    namedImports: ['TypeToShield', 'ShieldStr'],
  })

  const telefuncFileSource = project.getSourceFile(telefuncFilePath)
  assertTelefuncFilesSource(telefuncFileSource, { project, telefuncFilePath, tsConfigFilePath, appRootDir })
  // The code written in the file at `telefuncFilePath` isn't equal `telefuncFileCode` because of transforms
  telefuncFileSource.replaceWithText(telefuncFileCode)

  return { project, shieldGenSource }
}

function generateShieldCode({
  project,
  shieldGenSource,
  telefuncFilePath,
  exportList,
}: {
  project: Project & { tsConfigFilePath: null | string }
  shieldGenSource: SourceFile
  telefuncFilePath: string
  // All exports of `.telefunc.js` files must be functions, thus we generate a shield() for each export.
  // If an export isn't a function then the error message is a bit ugly: https://github.com/brillout/telefunc/issues/142
  exportList: ExportList
}): string {
  shieldGenSource.addImportDeclaration({
    moduleSpecifier: getTelefuncFileImportPath(telefuncFilePath),
    namedImports: exportList.map((e) => e.exportName),
  })

  // Aliases are batched so reads below amortize into a single typecheck — any `addTypeAlias`
  // invalidates ts-morph's cache. `@ts-ignore` silences TS6196 on these internal aliases (issue #229).
  for (const e of exportList) {
    addAlias(shieldGenSource, `${e.exportName}Shield`, `TypeToShield<typeof ${e.exportName}>`)
    addAlias(shieldGenSource, `${e.exportName}Ret`, `Awaited<ReturnType<typeof ${e.exportName}>>`)
    addAlias(shieldGenSource, `${e.exportName}Args`, `Parameters<typeof ${e.exportName}>`)
  }

  project.resolveSourceFileDependencies()
  assert(project.compilerOptions.get().strict === true)

  // Discover. Every kind (main / return / args) is walked independently — a failure on one has
  // no bearing on the others. Each ends up as its own record downstream.
  type Site = { segments: string[]; name: string; expr: string }
  type Discovered = {
    e: ExportList[number]
    mainShield: string | undefined
    returnSites: Site[]
    argumentSites: Site[]
  }
  const discovered: Discovered[] = []
  for (const e of exportList) {
    const mainShield = evalAlias(shieldGenSource, `${e.exportName}Shield`)
    if (mainShield === 'NON_FUNCTION_EXPORT') continue
    const retAlias = getAlias(shieldGenSource, `${e.exportName}Ret`)
    const argsAlias = getAlias(shieldGenSource, `${e.exportName}Args`)
    const returnSites: Site[] = []
    const argumentSites: Site[] = []
    walkSites(retAlias.getType(), `${e.exportName}Ret`, retAlias, 'args', (t) => `TypeToShield<${t}>`, returnSites)
    // The emitted shields always use array form (wrapped in `[...]`) so the runtime
    // `verifyOuter` only has to handle one shape.
    walkSites(
      argsAlias.getType(),
      `${e.exportName}Args`,
      argsAlias,
      'return',
      (t) => `\`[\${ShieldStr<Awaited<ReturnType<${t}>>>}]\``,
      argumentSites,
    )
    discovered.push({ e, mainShield, returnSites, argumentSites })
  }

  // Combined-literal alias per side. Each side resolves independently of the others (and of the
  // main shield) — no implicit failure chain.
  for (const d of discovered) {
    if (d.returnSites.length > 0) addAlias(shieldGenSource, `${d.e.exportName}SubRet`, subAlias(d.returnSites))
    if (d.argumentSites.length > 0) addAlias(shieldGenSource, `${d.e.exportName}SubArgs`, subAlias(d.argumentSites))
  }

  let shieldCode = [
    'import { shield as __telefunc_shield, __applyReturnShields, __applyArgumentShields } from "telefunc";',
    'const __telefunc_t = __telefunc_shield.type;',
  ].join('\n')

  const processSide = (d: Discovered, sites: Site[], aliasSuffix: string, applyFn: string) => {
    if (sites.length === 0) return null
    const raw = evalAlias(shieldGenSource, `${d.e.exportName}${aliasSuffix}`)
    if (raw === undefined) return { code: '', failed: true }
    const values = raw.split(SHIELD_SEP)
    assert(
      values.length === sites.length,
      `${d.e.exportName}${aliasSuffix}: ${values.length} parts, expected ${sites.length}`,
    )
    const entries = formatEntries(sites, values)
    return { code: entries ? `\n${applyFn}(${d.e.localName}, { ${entries} });` : '', failed: false }
  }

  for (const d of discovered) {
    const common = { project, telefuncFilePath, telefunctionName: d.e.exportName }
    const ret = processSide(d, d.returnSites, 'SubRet', '__applyReturnShields')
    const args = processSide(d, d.argumentSites, 'SubArgs', '__applyArgumentShields')

    if (d.mainShield !== undefined)
      shieldCode += `\n__telefunc_shield(${d.e.localName}, ${d.mainShield}, { __autoGenerated: true });`
    if (ret) shieldCode += ret.code
    if (args) shieldCode += args.code

    generatedShields.push({ ...common, kind: ShieldKind.Main, failed: d.mainShield === undefined })
    if (ret) generatedShields.push({ ...common, kind: ShieldKind.Return, failed: ret.failed })
    if (args) generatedShields.push({ ...common, kind: ShieldKind.Args, failed: args.failed })
  }

  shieldGenSource.delete()
  return shieldCode + '\n'
}

/** Combined template-literal alias body for one side's sites — values come back via `split(SHIELD_SEP)`. */
function subAlias(sites: readonly { expr: string }[]): string {
  return `\`${sites.map((s) => `\${${s.expr}}`).join(SHIELD_SEP)}\``
}

/** Internal alias — read via ts-morph at build time. `@ts-ignore` silences TS6196 (issue #229). */
function addAlias(source: SourceFile, name: string, type: string) {
  return source.addTypeAlias({
    name,
    type,
    leadingTrivia: '// @ts-ignore Internal — read at build time via ts-morph getLiteralValue()\n',
  })
}

function getAlias(source: SourceFile, name: string): TypeAliasDeclaration {
  const alias = source.getTypeAlias(name)
  assert(alias, `Type alias \`${name}\` not found.`)
  return alias
}

function evalAlias(source: SourceFile, name: string): string | undefined {
  const val = getAlias(source, name).getType().getLiteralValue()
  assert(val === undefined || typeof val === 'string', `Type alias \`${name}\` resolved to non-string literal.`)
  return val
}

const TELEFUNC_SHIELDS_PROP = '__DEFINE_TELEFUNC_SHIELDS'

/** Separator for the combined-literal trick — unicode PUA, never appears in shield output. */
const SHIELD_SEP = '\uE000'

/** Walk the structural composition of `type`, pushing one shield site per shielded value into `out`.
 *  Shielded: callables (`callableName` / `callableExpr`) and types declaring `[__DEFINE_TELEFUNC_SHIELDS]`.
 *  Descends anonymous objects and tuples only; primitives, class instances, named types, and arrays
 *  terminate (no shields, no children worth inspecting). Uses `locationNode` to resolve generic
 *  parameters against their actual type arguments. */
function walkSites(
  type: Type,
  typeRef: string,
  locationNode: TypeAliasDeclaration,
  callableName: string,
  callableExpr: (typeRef: string) => string,
  out: { segments: string[]; name: string; expr: string }[],
  segments: string[] = [],
): void {
  if (type.getCallSignatures().length > 0) {
    out.push({ segments, name: callableName, expr: callableExpr(typeRef) })
    return
  }
  const shieldsProp = type.getProperty(TELEFUNC_SHIELDS_PROP)
  if (shieldsProp) {
    // Every entry gets a shield — no skips. `never`, `void`, `undefined` all map to concrete
    // runtime verifiers via `ShieldStr` (`__telefunc_t.never` rejects everything;
    // `__telefunc_t.const(undefined)` accepts only undefined). Skipping here would let a
    // non-TS client bypass the type contract for positions TypeScript marks uninhabited.
    for (const entry of shieldsProp.getTypeAtLocation(locationNode).getProperties()) {
      const name = entry.getName()
      out.push({ segments, name, expr: `\`[\${ShieldStr<${typeRef}['${TELEFUNC_SHIELDS_PROP}']['${name}']>}]\`` })
    }
    return
  }
  if (type.isTuple()) {
    type.getTupleElements().forEach((el, i) => {
      walkSites(el, `${typeRef}[${i}]`, locationNode, callableName, callableExpr, out, [...segments, String(i)])
    })
    return
  }
  if (type.isAnonymous()) {
    for (const prop of type.getProperties()) {
      const name = prop.getName()
      const propType = prop.getTypeAtLocation(locationNode)
      walkSites(propType, `${typeRef}['${name}']`, locationNode, callableName, callableExpr, out, [...segments, name])
    }
  }
}

/** Group parallel `sites` + `values` by path-key, returning `"path": { name: value, ... }` fragments
 *  joined by `, `. Returns `null` if `sites` is empty (emit nothing). */
function formatEntries(
  sites: readonly { segments: string[]; name: string }[],
  values: readonly string[],
): string | null {
  if (sites.length === 0) return null
  const byKey = new Map<string, string[]>()
  for (let i = 0; i < sites.length; i++) {
    const { segments, name } = sites[i]!
    const key = toPathKey(segments)
    let entries = byKey.get(key)
    if (!entries) byKey.set(key, (entries = []))
    entries.push(`${name}: ${values[i]}`)
  }
  return Array.from(byKey, ([key, entries]) => `${JSON.stringify(key)}: { ${entries.join(', ')} }`).join(', ')
}

async function testGenerateShield(telefuncFileCode: string): Promise<string> {
  const telefuncFilePath = `virtual-${getRandomId()}.telefunc.ts`
  const { project, shieldGenSource } = getProject(telefuncFilePath, telefuncFileCode, '/fake-user-root-dir/', true)
  objectAssign(project, { tsConfigFilePath: null })
  const exportList = await getExportList(telefuncFileCode)
  const shieldCode = generateShieldCode({
    project,
    shieldGenSource,
    telefuncFilePath,
    exportList,
  })
  return shieldCode
}

function getImportPath(importer: string, importedFile: string) {
  let importPath = path.relative(path.dirname(importer), importedFile)
  importPath = toImport(importPath)
  return importPath
}

function getTelefuncFileImportPath(telefuncFilePath: string) {
  let importPath = path.basename(telefuncFilePath)
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
  printSubShieldFailures(appRootDir)
  resultAlreadyLogged = true
  generatedShields.length = 0
}

function printFailures(appRootDir: string) {
  const failures = generatedShields.filter((s) => s.kind === ShieldKind.Main && s.failed)
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

function printSubShieldFailures(appRootDir: string) {
  const failures = generatedShields.filter((s) => s.kind !== ShieldKind.Main && s.failed)
  assertWarning(
    failures.length === 0,
    [
      'Main shield() succeeded but sub-shields (return/args data-flow shields) could not be fully generated for telefunction',
      failures.length === 1 ? '' : 's',
      ' ',
      formatGeneratedShields(failures, appRootDir),
      '.',
      ' Arg-shape validation still works; only nested data-flow validation is missing.',
      ' See https://telefunc.com/shield#typescript-automatic for more information.',
    ].join(''),
    { onlyOnce: true },
  )
}

function printSuccesses(appRootDir: string, logSuccessPrefix: string) {
  const successes = generatedShields.filter((s) => s.kind === ShieldKind.Main && !s.failed)
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

function findTsConfig(telefuncFilePath: string, appRootDir: string): string | null {
  assert(fs.existsSync(telefuncFilePath))
  assertPosixPath(telefuncFilePath)
  assertPosixPath(appRootDir)
  assert(telefuncFilePath.startsWith(appRootDir))
  let curr = telefuncFilePath
  do {
    const dir = path.dirname(curr)
    if (dir === curr) {
      return null
    }
    if (!dir.startsWith(appRootDir)) {
      return null
    }
    const tsConfigFilePath = path.join(dir, 'tsconfig.json')
    if (fs.existsSync(tsConfigFilePath)) {
      return tsConfigFilePath
    }
    curr = dir
  } while (true)
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
