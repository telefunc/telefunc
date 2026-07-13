export { getExportList }
export { getTelefunctionRef }
export type { ExportList }

import { init, parse } from 'es-module-lexer'

type ExportList = {
  exportName: string
  localName: string | null
  /** Set for `export { foo } from './a.js'` — there's no local binding, the telefunction lives in `source`. */
  reExport: null | { source: string; sourceName: string }
}[]

async function getExportList(src: string): Promise<ExportList> {
  await init
  const parseResult = parse(src)
  const imports = parseResult[0]
  const exports = parseResult[1]
  const exportList = exports.map((e) => {
    const exportName = e.n
    const localName = e.ln ?? null
    let reExport: null | { source: string; sourceName: string } = null
    if (localName === null) {
      // A re-export statement (`export { foo } from './a.js'`) is also listed in `imports` => find the
      // import record containing this export to retrieve the source module.
      const importRecord = imports.find((i) => typeof i.n === 'string' && i.ss <= e.s && e.e <= i.se)
      if (importRecord) {
        reExport = { source: importRecord.n!, sourceName: getReExportSourceName(src, importRecord.ss, e) }
      }
    }
    return { exportName, localName, reExport }
  })
  return exportList
}

/**
 * The identifier that generated code (telefunction decoration, shield()) can use to reference the
 * telefunction. For `export { foo } from './a.js'` there's no local binding — the decoration code then
 * imports the telefunction from its source module under this deterministic alias (the shield() code,
 * generated separately, references the same alias).
 */
function getTelefunctionRef(e: ExportList[number]): string {
  if (e.localName) return e.localName
  if (e.reExport) return `__telefunc_reExport_${e.exportName}`
  return e.exportName
}

// For `export { foo as bar } from './a.js'` the lexer only provides the exported name (`bar`, at offsets
// e.s–e.e) => scan the statement text backwards for a `foo as` prefix to get the name to import from the
// source module.
function getReExportSourceName(src: string, statementStart: number, e: { s: number; n: string }): string {
  const beforeExportName = src.slice(statementStart, e.s)
  const aliasMatch = /([^\s{,]+)\s+as\s*$/.exec(beforeExportName)
  return aliasMatch ? aliasMatch[1]! : e.n
}
