import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { disposeBackend, installBackend } from '../backend/install.js'
import { MemoryBackend } from '../backend/memory/backend.js'
import { Room } from './server.js'

let backend: MemoryBackend

beforeEach(async () => {
  await disposeBackend()
  backend = new MemoryBackend()
  installBackend(() => backend)
})

afterEach(async () => {
  await disposeBackend()
})

async function currentHead(id: string) {
  return (await backend.readHead(id))?.head ?? null
}

describe('Room lifecycle is authority-owned (regressions that fail on 00f838b)', () => {
  it('a join is rejected against a closed room even through a pre-close handle', async () => {
    const room = await Room.create('la:stale-join')
    await Room.close('la:stale-join')

    await expect(room.join({ meta: { name: 'Alice' } })).rejects.toThrow(/closed/i)
    expect((await currentHead('la:stale-join'))?.state).toBe('closed')
  })

  it('setMeta on a closed room does not resurrect it', async () => {
    await Room.create('la:resurrect')
    await Room.close('la:resurrect')

    await expect(Room.setMeta('la:resurrect', { topic: 'zombie' })).rejects.toThrow(/closed|not found/i)
    expect((await currentHead('la:resurrect'))?.state).toBe('closed')
  })

  it('recreation is a fresh incarnation and carries none of the previous roster', async () => {
    const first = await Room.create('la:recreate')
    await first.join({ meta: { name: 'Alice' } })
    expect((await Room.getParticipants('la:recreate')).length).toBe(1)
    const firstInc = (await currentHead('la:recreate'))?.currentInc
    expect(firstInc).toBeTruthy()

    await Room.close('la:recreate')
    await Room.create('la:recreate')

    const recreated = await currentHead('la:recreate')
    expect(recreated?.state).toBe('open')
    expect(recreated?.currentInc).not.toBe(firstInc)
    expect(await Room.getParticipants('la:recreate')).toEqual([])
  })

  it('getOrCreate repairs a room directory entry that a partial create left behind', async () => {
    const room = await Room.create('la:index')
    expect((await Room.list()).map((entry) => entry.id)).toContain('la:index')

    await backend.directoryDelete('la:index', room._inc)
    expect((await Room.list()).map((entry) => entry.id)).not.toContain('la:index')

    await Room.getOrCreate('la:index')
    expect((await Room.list()).map((entry) => entry.id)).toContain('la:index')
  })
})
