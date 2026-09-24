export { assert }
export { assertUsage }
export { assertWarning }
export { assertInfo }
export { getProjectError }
export { errorPrefix }

import { createErrorWithCleanStackTrace } from './createErrorWithCleanStackTrace.js'
import { getGlobalObject } from './getGlobalObject.js'
import { projectInfo } from './projectInfo.js'
import pc from '@brillout/picocolors'

const errorPrefix = `[telefunc@${projectInfo.projectVersion}]`
const internalErrorPrefix = red(`${errorPrefix}[Bug]`)
const usageErrorPrefix = red(`${errorPrefix}[Wrong Usage]`)
const warningPrefix = yellow(`${errorPrefix}[Warning]`)
const infoPrefix = blue(`${errorPrefix}[Info]`)

const numberOfStackTraceLinesToRemove = 2

function assert(condition: unknown, debugInfo?: unknown): asserts condition {
  if (condition) {
    return
  }

  const debugStr = (() => {
    if (!debugInfo) {
      return ''
    }
    const debugInfoSerialized = typeof debugInfo === 'string' ? debugInfo : '`' + JSON.stringify(debugInfo) + '`'
    return `Debug info (this is for the ${projectInfo.projectName} maintainers; you can ignore this): ${debugInfoSerialized}.`
  })()

  const link = 'https://github.com/telefunc/telefunc/issues/new'
  const internalError = createErrorWithCleanStackTrace(
    [
      internalErrorPrefix,
      `You stumbled upon a Telefunc bug. Go to ${link} and copy-paste this error. A maintainer will fix the bug (usually under 24 hours).`,
      debugStr,
    ].join(' '),
    numberOfStackTraceLinesToRemove,
  )

  throw internalError
}

function assertUsage(condition: unknown, errorMessage: string): asserts condition {
  if (condition) {
    return
  }
  const whiteSpace = errorMessage.startsWith('[') ? '' : ' '
  const usageError = createErrorWithCleanStackTrace(
    `${usageErrorPrefix}${whiteSpace}${errorMessage}`,
    numberOfStackTraceLinesToRemove,
  )
  throw usageError
}

function getProjectError(errorMessage: string) {
  const sep = errorMessage.startsWith('[') ? '' : ' '
  const pluginError = createErrorWithCleanStackTrace(
    `${errorPrefix}${sep}${errorMessage}`,
    numberOfStackTraceLinesToRemove,
  )
  return pluginError
}

const globalObject = getGlobalObject<{ alreadyLogged: Set<string> }>('assert.ts', { alreadyLogged: new Set() })
function assertWarning(
  condition: unknown,
  errorMessage: string,
  { onlyOnce, showStackTrace }: { onlyOnce: boolean | string; showStackTrace?: true },
): void {
  if (condition) {
    return
  }
  const msg = `${warningPrefix} ${errorMessage}`
  if (onlyOnce) {
    const { alreadyLogged } = globalObject
    const key = onlyOnce === true ? msg : onlyOnce
    if (alreadyLogged.has(key)) {
      return
    } else {
      alreadyLogged.add(key)
    }
  }
  if (showStackTrace) {
    console.warn(new Error(msg))
  } else {
    console.warn(msg)
  }
}

function assertInfo(condition: unknown, errorMessage: string, { onlyOnce }: { onlyOnce: boolean }): void {
  if (condition) {
    return
  }
  const msg = `${infoPrefix} ${errorMessage}`
  if (onlyOnce) {
    const { alreadyLogged } = globalObject
    const key = msg
    if (alreadyLogged.has(key)) {
      return
    } else {
      alreadyLogged.add(key)
    }
  }
  console.log(msg)
}

function red<Str extends string>(str: Str) {
  return pc.red(pc.bold(str))
}
function yellow<Str extends string>(str: Str) {
  return pc.yellow(pc.bold(str))
}
function blue<Str extends string>(str: Str) {
  return pc.blue(pc.bold(str))
}
