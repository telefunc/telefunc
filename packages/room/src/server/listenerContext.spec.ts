import { describe, expect, it } from 'vitest'
import { getContext, provideTelefuncContext } from 'telefunc'
import { Room } from '../index.js'

/** Whose context `getContext()` gives where it's called, or that it throws. */
function whoseContext(): string {
  try {
    return getContext<{ user: string }>().user
  } catch {
    return 'throws'
  }
}

describe("a server-side room's callbacks", () => {
  it("subscribed in alice's context, get no context for bob's publish or one in no context", async () => {
    const id = `context-${crypto.randomUUID()}`
    const room = await Room.create(id)
    const read: string[] = []
    provideTelefuncContext({ user: 'alice' })
    room.subscribe(() => read.push(whoseContext()))
    provideTelefuncContext({ user: 'bob' })
    await Room.publish(id, 'hello')
    // A context provided without AsyncLocalStorage lasts until the next macrotask.
    await new Promise((resolve) => setTimeout(resolve, 0))
    await Room.publish(id, 'hello')
    expect(read).toEqual(['throws', 'throws'])
  })

  it("listen(), added in alice's context, gets no context for bob's private message that asks for a reply", async () => {
    const room = await Room.create(`context-${crypto.randomUUID()}`)
    const alice = await room.join({ meta: { name: 'alice' } })
    const bob = await room.join({ meta: { name: 'bob' } })
    provideTelefuncContext({ user: 'alice' })
    alice.listen(() => whoseContext())
    provideTelefuncContext({ user: 'bob' })
    await expect(bob.send(alice.id, 'hello', { ack: true })).resolves.toBe('throws')
  })
})
