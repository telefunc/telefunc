import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import ts from 'typescript'

const here = path.dirname(fileURLToPath(import.meta.url))
const root = path.resolve(here, '../..')
const snapshotPath = path.join(here, 'released-exports.json')
const snapshot = JSON.parse(fs.readFileSync(snapshotPath, 'utf8'))
const packageRoots = [
  ['telefunc', path.join(root, 'packages/telefunc')],
  ['@telefunc/redis', path.join(root, 'packages/redis')],
]
const forbiddenBackendExports = new Set(
  'BACKEND_SPI_VERSION BackendDriverPair BackendFactory BackendReceiver BroadcastDriver BroadcastLane CellMutation CommitAccepted CommitResult CxResult HeadCx HeadNext LaneId PublishResult RoomDriver RoomHead RoomSubscriptionSource SubscriptionAttempt SubscriptionAttemptState SubscriptionBinding SubscriptionDriver SubscriptionState installBackend'.split(
    ' ',
  ),
)

const publicEntryPoints = packageRoots.flatMap(([packageName, packageRoot]) => {
  const packageJson = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8'))
  return Object.keys(packageJson.exports)
    .filter((key) => key !== './package.json' && !key.startsWith('./__internal'))
    .map((key) => (key === '.' ? packageName : `${packageName}/${key.slice(2)}`))
})

const expectedEntryPoints = Object.keys(snapshot)
const missingSnapshots = publicEntryPoints.filter((entryPoint) => !expectedEntryPoints.includes(entryPoint))
const removedEntryPoints = expectedEntryPoints.filter((entryPoint) => !publicEntryPoints.includes(entryPoint))
if (missingSnapshots.length > 0 || removedEntryPoints.length > 0) {
  fail([
    formatDifference('Public entry points missing from the released snapshot', missingSnapshots),
    formatDifference('Snapshotted entry points missing from package exports', removedEntryPoints),
  ])
}

const probePath = path.join(here, '__released_exports_probe__.mts')
const probeSource = publicEntryPoints
  .map((entryPoint, index) => `import * as entryPoint${index} from ${JSON.stringify(entryPoint)}`)
  .join('\n')
const compilerOptions = {
  noEmit: true,
  skipLibCheck: true,
  strict: true,
  target: ts.ScriptTarget.ES2019,
  module: ts.ModuleKind.Node16,
  moduleResolution: ts.ModuleResolutionKind.Node16,
  types: [],
}
const host = ts.createCompilerHost(compilerOptions)
const originalGetSourceFile = host.getSourceFile.bind(host)
const isProbe = (fileName) => path.resolve(fileName).toLowerCase() === probePath.toLowerCase()
host.fileExists = (fileName) => isProbe(fileName) || ts.sys.fileExists(fileName)
host.readFile = (fileName) => (isProbe(fileName) ? probeSource : ts.sys.readFile(fileName))
host.getSourceFile = (fileName, languageVersion, onError, shouldCreateNewSourceFile) =>
  isProbe(fileName)
    ? ts.createSourceFile(probePath, probeSource, languageVersion, true, ts.ScriptKind.TS)
    : originalGetSourceFile(fileName, languageVersion, onError, shouldCreateNewSourceFile)

const program = ts.createProgram([probePath], compilerOptions, host)
const diagnostics = ts.getPreEmitDiagnostics(program).filter(({ category }) => category === ts.DiagnosticCategory.Error)
if (diagnostics.length > 0) {
  console.error(
    ts.formatDiagnostics(diagnostics, {
      getCanonicalFileName: (fileName) => fileName,
      getCurrentDirectory: () => root,
      getNewLine: () => '\n',
    }),
  )
  process.exit(1)
}

const checker = program.getTypeChecker()
const probe = program.getSourceFile(probePath)
const differences = []
publicEntryPoints.forEach((entryPoint, index) => {
  const namespace = probe.statements[index].importClause.namedBindings.name
  const alias = checker.getSymbolAtLocation(namespace)
  const moduleSymbol = checker.getAliasedSymbol(alias)
  const actual = checker
    .getExportsOfModule(moduleSymbol)
    .map((symbol) => symbol.getName())
    .sort()
  const expected = [...snapshot[entryPoint]].sort()
  differences.push(
    formatDifference(
      `${entryPoint}: missing released exports`,
      expected.filter((name) => !actual.includes(name)),
    ),
    formatDifference(
      `${entryPoint}: exports absent from the snapshot`,
      actual.filter((name) => !expected.includes(name)),
    ),
  )
  if (entryPoint === 'telefunc/backend')
    differences.push(
      formatDifference(
        `${entryPoint}: forbidden deferred exports`,
        actual.filter((name) => forbiddenBackendExports.has(name)),
      ),
    )
})

fail(differences)

function formatDifference(label, values) {
  return values.length === 0 ? '' : `${label}: ${values.join(', ')}`
}

function fail(messages) {
  const failures = messages.filter(Boolean)
  if (failures.length === 0) return
  console.error(failures.join('\n'))
  process.exit(1)
}
