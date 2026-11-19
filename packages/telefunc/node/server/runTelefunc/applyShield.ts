export { applyShield }

import { shieldApply, shieldIsMissing, logShieldError } from '../shield.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { assertWarning } from '../../../utils/assert.js'
import { isProduction } from '../../../utils/isProduction.js'
import type { Telefunction } from '../types.js'
import type { ConfigResolved } from '../serverConfig.js'

function applyShield(runContext: {
  telefunction: Telefunction
  telefunctionName: string
  telefuncFilePath: string
  telefunctionArgs: unknown[]
  serverConfig: Pick<ConfigResolved, 'log'>
}): { isValidRequest: boolean } {
  const { telefunction, telefunctionArgs, telefunctionName, telefuncFilePath } = runContext

  const hasShield = !shieldIsMissing(telefunction)
  const isStandalone = telefuncFilePath === '' || !!(telefunction as any)._isStandalone

  if (isStandalone) {
    if (!hasShield) {
      if (telefunctionArgs.length > 0) {
        logShieldError(
          {
            telefunctionName,
            telefuncFilePath,
            subject: 'the arguments passed',
            wrongType: `Expected 0 arguments because standalone telefunction has no shield(), but received ${telefunctionArgs.length}`,
            extra: `Arguments: \`${stringify(telefunctionArgs)}\`.`,
          },
          runContext.serverConfig.log.shieldErrors,
        )
        return { isValidRequest: false }
      }
      return { isValidRequest: true }
    }
  } else {
    if (isProduction()) {
      assertWarning(
        hasShield || telefunctionArgs.length === 0,
        `The telefunction ${telefunctionName}() (${telefuncFilePath}) accepts arguments yet is missing shield(), see https://telefunc.com/shield`,
        { onlyOnce: true },
      )
    }
    if (!hasShield) {
      return { isValidRequest: true }
    }
  }

  const applyResult = shieldApply(telefunction, telefunctionArgs)
  if (applyResult === true) {
    return { isValidRequest: true }
  }

  logShieldError(
    {
      telefunctionName,
      telefuncFilePath,
      subject: 'the arguments passed',
      wrongType: applyResult,
      extra: `Arguments: \`${stringify(telefunctionArgs)}\`.`,
    },
    runContext.serverConfig.log.shieldErrors,
  )

  return { isValidRequest: false }
}
