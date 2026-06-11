export { close, setCloseHandlers, getCloseHandlers, addExtraCloseHandlers }

import { assertUsage } from '../utils/assert.js'
import { isObjectOrFunction } from '../utils/isObjectOrFunction.js'

const CLOSE_HANDLERS = Symbol.for('telefuncCloseHandlers')
const EXTRA_CLOSE_HANDLERS = Symbol.for('telefuncExtraCloseHandlers')

export type CloseHandler = () => void | Promise<unknown>
type CloseHandlers = WeakMap<object, CloseHandler>
type ExtraCloseHandlers = Array<CloseHandler>
type WithCloseHandlers = { [CLOSE_HANDLERS]?: CloseHandlers; [EXTRA_CLOSE_HANDLERS]?: ExtraCloseHandlers }

function setCloseHandlers(value: object, closeHandlers: CloseHandlers): void {
  const p = value as WithCloseHandlers
  p[CLOSE_HANDLERS] = closeHandlers
}

function addExtraCloseHandlers(value: object, closeHandlers: ExtraCloseHandlers): void {
  if (closeHandlers.length === 0) return
  const p = value as WithCloseHandlers
  p[EXTRA_CLOSE_HANDLERS] = closeHandlers
}

async function close(value: object): Promise<void> {
  assertUsage(isObjectOrFunction(value), '`close()`: the argument must be a telefunc return value')
  const closeHandlers = getCloseHandlers(value)
  assertUsage(closeHandlers, '`close()`: the argument is not a closable telefunc return value')
  const extraHandlers = getExtraCloseHandlers(value)
  await Promise.all([
    ...(extraHandlers ?? []).map((h) => h()),
    closeRecursively(value, closeHandlers, new WeakSet<object>()),
  ])
}

async function closeRecursively(value: object, closeHandlers: CloseHandlers, seen: WeakSet<object>): Promise<boolean> {
  if (seen.has(value)) return false
  seen.add(value)

  let closedAny = false
  const pending: Promise<void>[] = []
  const closeHandler = closeHandlers.get(value)
  if (closeHandler) {
    pending.push(Promise.resolve(closeHandler()).then(() => undefined))
    closedAny = true
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      if (!isObjectOrFunction(item)) continue
      pending.push(
        closeRecursively(item, closeHandlers, seen).then((didClose) => {
          if (didClose) closedAny = true
        }),
      )
    }
    await Promise.all(pending)
    return closedAny
  }

  for (const item of Object.values(value)) {
    if (!isObjectOrFunction(item)) continue
    pending.push(
      closeRecursively(item, closeHandlers, seen).then((didClose) => {
        if (didClose) closedAny = true
      }),
    )
  }

  await Promise.all(pending)
  return closedAny
}

function getCloseHandlers(value: object): CloseHandlers | undefined {
  const p = value as WithCloseHandlers
  return p[CLOSE_HANDLERS]
}

function getExtraCloseHandlers(value: object): ExtraCloseHandlers | undefined {
  const p = value as WithCloseHandlers
  return p[EXTRA_CLOSE_HANDLERS]
}
