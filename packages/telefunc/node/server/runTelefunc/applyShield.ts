export { applyShield }

import { shieldApply, shieldIsMissing, logShieldError } from '../shield.js'
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
      extra: `Arguments: \`${JSON.stringify(telefunctionArgs)}\`.`,
    },
    runContext.serverConfig.log.shieldErrors,
  )

  return { isValidRequest: false }
}
