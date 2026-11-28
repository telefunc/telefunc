// The commands the backend sends without a script: each one's Redis name, whether it may write, whether its reply's bulk
// strings stay bytes, and its reply as the backend reads it.
export { RAW_COMMANDS, type RawCommand }

import { bytes, list, number, text, texts, twoOf } from './replies.js'
import { rawWrites } from './ioredis.js'

/** A command sent without a script. It may write as ioredis's own table of Redis's commands says (`rawWrites()`). */
type RawCommand<Output> = {
  readonly name: string
  readonly writes: boolean
  /** Its reply's bulk strings stay bytes. */
  readonly bytes: boolean
  parse(reply: unknown): Output
}

const raw = <Output>(name: string, bytes: boolean, parse: (reply: unknown) => Output): RawCommand<Output> => ({
  name,
  writes: rawWrites(name),
  bytes,
  parse,
})

function bytesOrNull(reply: unknown): Buffer | null {
  return reply === null ? null : bytes(reply)
}

function textOrNull(reply: unknown): string | null {
  return reply === null ? null : text(reply)
}

function textsOrNulls(reply: unknown): (string | null)[] {
  return list(reply).map(textOrNull)
}

/** A page of SSCAN: the cursor of the next page, `'0'` after the last, and the members of this one. */
function scanPage(reply: unknown): [cursor: string, members: string[]] {
  const [cursor, members] = twoOf(list(reply))
  return [text(cursor), texts(members)]
}

/** By what they read: `getBytes` is GET, its reply bytes. */
const RAW_COMMANDS = {
  getBytes: raw('get', true, bytesOrNull),
  hget: raw('hget', false, textOrNull),
  hgetBytes: raw('hget', true, bytesOrNull),
  hkeys: raw('hkeys', false, texts),
  hmget: raw('hmget', false, textsOrNulls),
  smembers: raw('smembers', false, texts),
  sscan: raw('sscan', false, scanPage),
  unlink: raw('unlink', false, number),
  zrangebylex: raw('zrangebylex', false, texts),
}
