export { getExportList }
export type { ExportList }

import { init, parse, type ImportSpecifier } from 'es-module-lexer'
import { assertUsage } from '../../../utils/assert.js'

type ExportList = { exportName: string; localName: string | null }[]

async function getExportList(src: string, filePath: string): Promise<ExportList> {
  await init
  const parseResult = parse(src)
  assertNoReExport(src, parseResult[0], filePath)
  const exports = parseResult[1]
  const exportList = exports.map((e) => {
    const exportName = e.n
    const localName = e.ln ?? null
    return { exportName, localName }
  })
  return exportList
}

// `export { onFoo } from './impl.js'` creates no local binding the generated code (telefunction
// decoration, shield()) could reference — the telefunction would crash at runtime instead of getting
// registered. Not worth supporting (https://github.com/telefunc/telefunc/issues/462) => usage error.
// Type-only re-exports (`export type { T } from './impl.js'`) are fine: es-module-lexer skips them.
function assertNoReExport(src: string, imports: readonly ImportSpecifier[], filePath: string) {
  for (const i of imports) {
    if (i.d !== -1) continue // dynamic import or import.meta
    // Re-export statements (`export { onFoo } from './impl.js'`, `export * from './impl.js'`, ...) are
    // also listed in `imports` — they're the import records starting with the `export` keyword.
    const statement = src.slice(i.ss, i.se)
    assertUsage(
      !/^export\b/.test(statement),
      `The telefunc file ${filePath} contains \`${statement}\` which isn't supported — define telefunctions in the .telefunc file itself instead of re-exporting them. If you have a use case for re-exports, comment at https://github.com/telefunc/telefunc/issues/462`,
    )
  }
}
