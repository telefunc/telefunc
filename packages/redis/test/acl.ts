// The Redis page's ACL rule for Telefunc's own user, as the ACL store specs read and run it.
export {
  documentedRule,
  CLIENT_INFO_GRANT,
  knowsClientInfo,
  setUser,
  refused,
  reaches,
  calls,
  outcomeOf,
  EVERY_CALL_RUNS,
}

import { readFileSync } from 'node:fs'
import { setTimeout as sleep } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import type { Redis } from 'ioredis'
import { BackendError, Broadcast } from 'telefunc'
import { Room } from '@telefunc/room'
import type { Store } from './stores.js'

/** The grant the page gives as optional, on the stores that know `CLIENT SETINFO`. */
const CLIENT_INFO_GRANT = '+client|setinfo'

/** Whether the store knows `CLIENT SETINFO`, which ioredis sends on every connection it opens: Redis 7.2 and later, and
 *  Valkey. */
const knowsClientInfo = (store: Store) => store.name !== 'Redis 7.0.0'

/** The rule's words after `ACL SETUSER telefunc`, as the page writes them, with the password the rows log in with. */
function documentedRule(): string[] {
  const page = readFileSync(fileURLToPath(new URL('../../../docs/pages/redis/+Page.mdx', import.meta.url)), 'utf8')
  const line = page.split('\n').find((text) => text.startsWith('ACL SETUSER telefunc '))
  if (line === undefined) throw new Error('the Redis page gives no `ACL SETUSER telefunc` rule')
  return line
    .split(' ')
    .slice(3)
    .map((word) => (word === '>your-password' ? '>pw' : word))
}

/** What `run` resolves with, or what it throws or rejects with. */
async function settled<T>(run: () => Promise<T>): Promise<{ ok: true; value: T } | { ok: false; error: unknown }> {
  try {
    return { ok: true, value: await run() }
  } catch (error) {
    return { ok: false, error }
  }
}

/** What the store answers `ACL SETUSER telefunc <rule>`. */
async function setUser(admin: Redis, rule: string[]): Promise<string> {
  const answer = await settled(() => admin.call('ACL', 'SETUSER', 'telefunc', ...rule))
  if (answer.ok) return String(answer.value)
  return answer.error instanceof Error ? answer.error.message : String(answer.error)
}

/** What the stores' ACL LOGs say they refused, each entry once. */
async function refused(admins: Redis[]): Promise<string[]> {
  const seen = new Set<string>()
  for (const admin of admins) {
    const entries = await admin.call('ACL', 'LOG', '128')
    for (const entry of Array.isArray(entries) ? entries : []) {
      const fields: unknown[] = Array.isArray(entry) ? entry : []
      const field = (name: string) => String(fields[fields.indexOf(name) + 1])
      seen.add(`${field('reason')} ${field('object')}`)
    }
  }
  return [...seen].sort()
}

/** Once a read of a room that isn't there gets past the store, or the store refuses it the user. */
async function reaches(name: string): Promise<void> {
  const answers = async () => {
    const read = await settled(() => Room.get('r-none'))
    return read.ok || !(read.error instanceof BackendError) || read.error.code === 'denied'
  }
  for (let tries = 0; !(await answers()); tries++) {
    if (tries > 400) throw new Error(`the backend never reached ${name}`)
    await sleep(25)
  }
}

/** A call's outcome: `ok`, or its error's name and message. */
async function outcomeOf(run: () => Promise<unknown>): Promise<string> {
  const done = await settled(run)
  if (done.ok) return 'ok'
  return done.error instanceof Error ? `${done.error.name}: ${done.error.message}` : String(done.error)
}

/** Every call Telefunc makes of the store, each one's outcome: `ok`, or its error's name and message. `tag` names the
 *  channel and the room of one round of the calls apart from another's. */
async function calls(tag = ''): Promise<Record<string, string>> {
  const told: Record<string, string> = {}
  const call = async (name: string, run: () => Promise<unknown>) => {
    told[name] = await outcomeOf(run)
  }
  const channel = `acl${tag}`
  const received: unknown[] = []
  const listening = Broadcast.subscribe(channel, (message) => received.push(message))
  await call('Broadcast.subscribe', () => listening.ready)
  await call('Broadcast.publish', async () => {
    await Broadcast.publish(channel, 'hello')
    if (!(await arrived(received))) throw new Error('the message never arrived')
  })
  listening()
  const frames: Uint8Array[] = []
  const listeningBinary = Broadcast.subscribeBinary(channel, (frame) => frames.push(frame))
  await call('Broadcast.subscribeBinary', () => listeningBinary.ready)
  await call('Broadcast.publishBinary', async () => {
    await Broadcast.publishBinary(channel, new Uint8Array([1]))
    if (!(await arrived(frames))) throw new Error('the frame never arrived')
  })
  listeningBinary()
  const id = `acl-room${tag}`
  await call('Room.create', () => Room.create(id))
  await call('join, and a private message with its reply', async () => {
    const alice = await Room.join(id, { identity: 'alice' })
    const bob = await Room.join(id)
    bob.listen(() => 'pong')
    const reply = await alice.send(bob.id, 'ping', { ack: true })
    if (reply !== 'pong') throw new Error(`the reply was ${String(reply)}`)
  })
  await call('a track subscription, and a binary publish on the track', async () => {
    const room = await Room.getOrCreate(id)
    const trackFrames: Uint8Array[] = []
    const stop = room.subscribeBinary((frame) => trackFrames.push(frame), { track: 'acl' })
    const carol = await room.join()
    // The track's subscription goes out after the call returns: the frame is published again until one arrives.
    for (let tries = 0; ; tries++) {
      await carol.publishBinary(new Uint8Array([1]), { track: 'acl' })
      if (await arrived(trackFrames, 10)) break
      if (tries > 40) throw new Error('no frame on the track arrived')
    }
    stop()
  })
  await call('Room.setMeta', () => Room.setMeta(id, { a: '1' }))
  await call('Room.updateMeta', () => Room.updateMeta(id, { b: '2' }))
  await call('Room.publish', () => Room.publish(id, 'hi'))
  await call('Room.list', () => Room.list())
  await call('Room.getParticipants', () => Room.getParticipants(id))
  await call('Room.removeParticipant', () => Room.removeParticipant(id, { identity: 'alice' }))
  await call('Room.close', () => Room.close(id))
  return told
}

/** Whether `list` holds something within `ticks` waits of 25 ms. */
async function arrived(list: readonly unknown[], ticks = 200): Promise<boolean> {
  for (let tries = 0; list.length === 0; tries++) {
    if (tries >= ticks) return false
    await sleep(25)
  }
  return true
}

const EVERY_CALL_RUNS = Object.fromEntries(
  [
    'Broadcast.subscribe',
    'Broadcast.publish',
    'Broadcast.subscribeBinary',
    'Broadcast.publishBinary',
    'Room.create',
    'join, and a private message with its reply',
    'a track subscription, and a binary publish on the track',
    'Room.setMeta',
    'Room.updateMeta',
    'Room.publish',
    'Room.list',
    'Room.getParticipants',
    'Room.removeParticipant',
    'Room.close',
  ].map((name) => [name, 'ok']),
)
