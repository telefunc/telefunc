import { describe, expect, it } from 'vitest'
import { LiveCell, derived, type LiveSource } from './live.js'

/** A `.live()` query's reads, made stale by hand. */
class FakeSource implements LiveSource {
  readonly listeners = new Set<() => void>()

  subscribe(onStale: () => void): () => void {
    this.listeners.add(onStale)
    return () => this.listeners.delete(onStale)
  }

  stale(): void {
    for (const listener of this.listeners) listener()
  }
}

describe('LiveCell', () => {
  it('sends its value once, then keeps none', () => {
    const todos = new LiveCell(['First todo'], [new FakeSource()])
    expect(todos.send()).toEqual(['First todo'])
    expect(todos.data).toBe(undefined)
    expect(() => todos.send()).toThrow('A `Live` is sent once')
  })

  it('watches its sources from when it is sent until it is released', async () => {
    const source = new FakeSource()
    const todos = new LiveCell(['First todo'], [source])
    let stale = 0
    todos.onStale(() => stale++)
    expect(source.listeners.size).toBe(0)

    todos.send()
    source.stale()
    source.stale()
    await Promise.resolve()
    expect(stale).toBe(1)

    todos.release()
    expect(source.listeners.size).toBe(0)
  })

  it('goes stale when a source it was derived from does, without keeping the values it read', async () => {
    const todosSource = new FakeSource()
    const notesSource = new FakeSource()
    const todos = new LiveCell(['First todo'], [todosSource])
    const notes = new LiveCell(['Note'], [notesSource])
    // The server side of what `derived()` returns.
    const summary = derived(() => `${todos.data.length} todos, ${notes.data.length} notes`) as LiveCell<string>
    let stale = 0
    summary.onStale(() => stale++)

    expect(summary.send()).toBe('1 todos, 1 notes')
    expect(todosSource.listeners.size).toBe(1)
    notesSource.stale()
    await Promise.resolve()
    expect(stale).toBe(1)
    // `summary` reaches the sources only: `todos` and `notes` are never sent, and nothing holds them.
    expect(Object.values(summary).flat()).not.toContain(todos)
  })
})
