export { assertNamingConvention }

import { import_ } from '@brillout/import'
import { assertWarning } from '../../../utils/assert.js'
import { isCloudflareWorkers } from '../../../utils/isCloudflareWorkers.js'
import { isProduction } from '../../../utils/isProduction.js'
import { assertPosixPath } from '../../../utils/path.js'
import type * as fsType from 'node:fs'
import type * as pathType from 'node:path'

async function assertNamingConvention(
  exportValue: unknown,
  exportName: string,
  telefuncFilePath: string,
  appRootDir: null | string,
): Promise<void> {
  if (isProduction()) return
  assertStartsWithOn(exportName, telefuncFilePath)
  await assertCollocation(telefuncFilePath, appRootDir, exportValue)
}

function assertStartsWithOn(exportName: string, telefuncFilePath: string) {
  if (/on[A-Z]/.test(exportName)) return
  if (!/on/.test(exportName)) {
    assertWarning(
      false,
      `We recommend the name of your telefunction ${exportName}() (${telefuncFilePath}) to start with "on", see https://telefunc.com/event-based#naming-convention'`,
      { onlyOnce: true },
    )
  } else {
    assertWarning(
      /on[A-Z]/.test(exportName),
      `The name of your telefunction ${exportName}() (${telefuncFilePath}) starts with "on" but isn't followed by a capital letter, see https://telefunc.com/event-based#naming-convention'`,
      { onlyOnce: true },
    )
  }
}

// TO-DO/eventually: run it at build-time instead of runtime. We didn't do this yet because `config.disableNamingConvention` is a server-side runtime config — it'd need to be defined at build-time instead.
async function assertCollocation(telefuncFilePath: string, appRootDir: string | null, exportValue: unknown) {
  appRootDir = appRootDir || ((exportValue as any)._appRootDir as undefined | string) || null
  if (!appRootDir) return
  if (isCloudflareWorkers()) return // Even though the modules 'node:fs' and 'node:path' are available inside Cloudflare Workers with 'nodejs_compat', the filesystem still isn't available and `Error: no such file or directory` is thrown (even if the file/directory does exist)

  let fs: typeof fsType
  let path: typeof pathType
  try {
    fs = await import_('node:fs')
    path = await import_('node:path')
  } catch {
    // The environment doesn't seem to have a filesystem API => skip `assertCollocation()`.
    // - For example, Cloudflare Workers doesn't have a filesystem API.
    return
  }

  const getBasename = (fileNameOrPath: string): string => {
    assertPosixPath(fileNameOrPath)
    let basename = path.posix.basename(fileNameOrPath).split('.')[0]!
    if (basename.startsWith('+')) basename = basename.slice(1)
    return basename
  }

  assertPosixPath(telefuncFilePath)
  const telefuncFileBasename = getBasename(telefuncFilePath)
  const telefuncFileDir = path.posix.dirname(telefuncFilePath)
  const telefuncFileDirAbsolute = path.posix.join(appRootDir, telefuncFileDir)
  const collocatedFiles = fs.readdirSync(telefuncFileDirAbsolute)
  const collocatedFilesMatchYes: string[] = []
  const collocatedFilesMatchNot: string[] = []
  collocatedFiles.forEach((fileName) => {
    assertPosixPath(fileName) // fileName isn't a path so it shouldn't contain any backslash windows path separator
    const fileBasename = getBasename(fileName)
    fileName = path.posix.join(telefuncFileDir, fileName)
    if (fileBasename === telefuncFileBasename) {
      collocatedFilesMatchYes.push(fileName)
    } else {
      collocatedFilesMatchNot.push(fileName)
    }
  })
  /* There seem to be a race condition: https://github.com/brillout/telefunc/issues/61
  assert(collocatedFilesMatchYes.length >= 1, { telefuncFilePathAbsolute, collocatedFiles })
  */
  assertWarning(
    collocatedFilesMatchYes.length >= 2,
    [
      `We recommend to collocate ${telefuncFilePath} with a UI component file, see https://telefunc.com/event-based#naming-convention`,
      '    Your telefunction:',
      `      ${telefuncFilePath} (base name: '${telefuncFileBasename}')`,
      '    Its collocated files:',
      ...collocatedFilesMatchNot.map((fileName) => `      ${fileName} (base name: '${getBasename(fileName)}'`),
      `    None of its collocated files share its base name '${telefuncFileBasename}'.`,
    ].join('\n'),
    { onlyOnce: true },
  )
}
