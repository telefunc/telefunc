export { close, setCloseHandlers, getCloseHandlers }

import type { ChannelCloseResult } from '../wire-protocol/channel.js'
import { assertUsage } from '../utils/assert.js'
import { isObjectOrFunction } from '../utils/isObjectOrFunction.js'

const CLOSE_HANDLERS = Symbol.for('telefuncCloseHandlers')

type CloseHandlers = WeakMap<object, () => void | Promise<ChannelCloseResult>>
type WithCloseHandlers = { [CLOSE_HANDLERS]?: CloseHandlers }

function setCloseHandlers(value: object, closeHandlers: CloseHandlers): void {
  const p = value as WithCloseHandlers
  p[CLOSE_HANDLERS] = closeHandlers
}

async function close(value: object): Promise<void> {
  assertUsage(isObjectOrFunction(value), '`close()`: the argument must be a telefunc return value')
  const closeHandlers = getCloseHandlers(value)
  assertUsage(closeHandlers, '`close()`: the argument is not a closable telefunc return value')
  const closedAny = await closeRecursively(value, closeHandlers, new WeakSet<object>())
  assertUsage(closedAny, '`close()`: the argument is not a closable telefunc return value')
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
