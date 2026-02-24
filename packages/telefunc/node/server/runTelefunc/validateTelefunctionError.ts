export { validateTelefunctionError }
export { handleTelefunctionBug }

import type { TelefuncIdentifier } from '../../../shared/constants.js'
import { Abort } from '../Abort.js'
import { assertUsage } from '../../../utils/assert.js'
import { callBugListeners } from './onBug.js'
import { handleError } from './handleError.js'

function validateTelefunctionError(err: unknown, telefuncId: TelefuncIdentifier): void {
  assertUsage(
    typeof err === 'object' && err !== null,
    `The telefunction ${telefuncId.telefunctionName}() (${telefuncId.telefuncFilePath}) threw a non-object error: \`${err}\`. Make sure the telefunction does \`throw new Error(${err})\` instead.`,
  )
  assertUsage(
    err !== Abort,
    `Missing parentheses \`()\` in \`throw Abort\` (it should be \`throw Abort()\`) at telefunction ${telefuncId.telefunctionName}() (${telefuncId.telefuncFilePath}).`,
  )
}

function handleTelefunctionBug(err: unknown): void {
  callBugListeners(err)
  handleError(err)
}
