export { close, setCloseHandlers, getCloseHandlers }

import { assertUsage } from '../utils/assert.js'
import { isObjectOrFunction } from '../utils/isObjectOrFunction.js'

const CLOSE_HANDLERS = Symbol.for('telefuncCloseHandlers')

type CloseHandlers = WeakMap<object, () => void>
type WithCloseHandlers = { [CLOSE_HANDLERS]?: CloseHandlers }

function setCloseHandlers(value: object, closeHandlers: CloseHandlers): void {
  const p = value as WithCloseHandlers
  p[CLOSE_HANDLERS] = closeHandlers
}

function close(value: object): void {
  assertUsage(isObjectOrFunction(value), '`close()`: the argument must be a telefunc return value')
  const closeHandlers = getCloseHandlers(value)
  assertUsage(closeHandlers, '`close()`: the argument is not a closable telefunc return value')
  const closedAny = closeRecursively(value, closeHandlers, new WeakSet<object>())
  assertUsage(closedAny, '`close()`: the argument is not a closable telefunc return value')
}

function closeRecursively(value: object, closeHandlers: CloseHandlers, seen: WeakSet<object>): boolean {
  if (seen.has(value)) return false
  seen.add(value)

  let closedAny = false
  const closeHandler = closeHandlers.get(value)
  if (closeHandler) {
    closeHandler()
    closedAny = true
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      if (isObjectOrFunction(item) && closeRecursively(item, closeHandlers, seen)) closedAny = true
    }
    return closedAny
  }

  for (const item of Object.values(value)) {
    if (isObjectOrFunction(item) && closeRecursively(item, closeHandlers, seen)) closedAny = true
  }

  return closedAny
}

function getCloseHandlers(value: object): CloseHandlers | undefined {
  const p = value as WithCloseHandlers
  return p[CLOSE_HANDLERS]
}
