export { transformTelefuncFileClientSideSync }

import { posix } from 'node:path'
import { assert, assertUsage } from '../../../utils/assert.js'
import { getTelefunctionKey } from '../../../utils/getTelefunctionKey.js'
import { assertPosixPath } from '../../../utils/path.js'
import { rollupSourceMapRemove } from '../../../utils/rollupSourceMap.js'

function transformTelefuncFileClientSideSync(id: string, appRootDir: string, exportNames: string[]) {
  assertPosixPath(id)
  assertPosixPath(appRootDir)

  let telefuncFilePath = posix.relative(appRootDir, id)
  assertPosixPath(telefuncFilePath)
  assertUsage(
    !telefuncFilePath.startsWith('../'),
    `The telefunc file ${telefuncFilePath} needs to live inside ${appRootDir} (the client-side root directory, i.e. the root directory of Vite/Vike/Next.js/Nuxt/...)`,
  )
  assert(!telefuncFilePath.startsWith('/') && !telefuncFilePath.startsWith('.'))
  telefuncFilePath = `/${telefuncFilePath}`

  const code = getCode(exportNames, telefuncFilePath)
  return rollupSourceMapRemove(code)
}

export function getCode(exportNames: readonly string[], telefuncFilePath: string) {
  const lines: string[] = []

  lines.push('// @ts-nocheck')

  lines.push(`import { __remoteTelefunctionCall } from 'telefunc/client';`)

  exportNames.forEach((exportName) => {
    const varName = exportName === 'default' ? 'defaultExport' : exportName

    lines.push(
      `const ${varName} =  (...args) => __remoteTelefunctionCall('${telefuncFilePath}', '${exportName}', args);`,
    )

    {
      const key = getTelefunctionKey(telefuncFilePath, exportName)
      lines.push(`${varName}._key = ${JSON.stringify(key)};`)
    }

    if (exportName === 'default') {
      lines.push(`export default ${varName};`)
    } else {
      lines.push(`export { ${varName} };`)
    }
  })

  const code = lines.join('\n')
  return code
}
