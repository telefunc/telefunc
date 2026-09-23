// Every Lua script and the one command table that registers and invokes them.

import {
  encodeLaneKey,
  type BroadcastLane,
  type CellMutation,
  type CxResult,
  type HeadCx,
  type HeadCxResult,
  type HeadNext,
  type LaneId,
  type RoomHead,
} from 'telefunc/__internal'
import { assert } from './assert.js'
import {
  broadcastChannel,
  broadcastSequenceKey,
  cellKey,
  cellKeyPrefix,
  channelKey,
  directoryIndexKey,
  directoryTagsKey,
  generationInvalidationChannel,
  generationKeysKey,
  gensKey,
  headKey,
  headRevKey,
  orderKey,
  retainedKey,
  revKey,
} from './keys.js'

export const REDIS_DELIVERY_FENCE_BYTE = 0xff

// Shared preamble: authority time in ms from Redis TIME's [sec, µs] pair, and the head read that applies it.
const HEAD_PRELUDE = `
local function tf_now()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
-- read the head, treating a logically-expired tombstone as absent (a lapsed tombstone reopens the
-- absence epoch); the PX backstop only reclaims memory, it is never what makes it invisible.
local function tf_read_and_expire_head(key, now)
  local raw = redis.call('GET', key)
  if not raw then return nil end
  local h = cjson.decode(raw)
  if h.exp and h.exp <= now then
    redis.call('DEL', key)
    return nil
  end
  return h
end
`

// The same bytes as telefunc's ordering frame (wire-protocol/ordering-frame.ts): four u32 big-endian words, then the payload.
export const REDIS_ORDERING_FRAME_LUA = `
local function tf_ordering_frame(seq, ts, payload)
  local seq_hi = math.floor(seq / 4294967296)
  local seq_lo = seq - seq_hi * 4294967296
  local ts_hi = math.floor(ts / 4294967296)
  local ts_lo = ts - ts_hi * 4294967296
  return struct.pack('>I4I4I4I4', seq_hi, seq_lo, ts_hi, ts_lo) .. payload
end
`

// Broadcast publish: next per-key seq, authority time, one PUBLISH of the ordering frame.
//   KEYS: [1]=sequence [2]=channel
//   ARGV: [1]=payload
const PUBLISH_LUA = `${REDIS_ORDERING_FRAME_LUA}
local previous = redis.call('GET', KEYS[1])
if previous and tonumber(previous) >= ${Number.MAX_SAFE_INTEGER} then
  return redis.error_reply('publish: sequence exhausted for the ordering domain')
end
local seq = redis.call('INCR', KEYS[1])
local t = redis.call('TIME')
local ts = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local frame = tf_ordering_frame(seq, ts, ARGV[1])
local receivers = redis.call('PUBLISH', KEYS[2], frame)
return {seq, ts, receivers}
`

// HEAD CX compares by form, then stores; core decides every transition and the supervisor checks its shape.
//   KEYS: [1]=head [2]=gens [3]=headrev
//   ARGV: [1]=HeadCx JSON {form,rev?,lease?} [2]=nextJson{state,inc?,config,lease?,ttlMs?}
export const HEAD_CX_LUA = `${HEAD_PRELUDE}
local head_key, gens_key, rev_key = KEYS[1], KEYS[2], KEYS[3]
local now = tf_now()
local cx = cjson.decode(ARGV[1])
local nx = cjson.decode(ARGV[2])
local cur = tf_read_and_expire_head(head_key, now)

local matches = false
if cx.form == 'absent' then
  matches = (cur == nil)
elseif cur ~= nil and cur.rev == cx.rev then
  if cx.form == 'takeover' then
    matches = (cur.state == 'closing' and cur.lease ~= nil and cur.lease['until'] < now)
  elseif cx.form == 'finalize' then
    matches = (cur.state == 'closing' and cur.lease ~= nil and cur.lease.id == cx.lease)
  else
    matches = true
  end
end
if not matches then
  if cur then return '{"tag":"conflict","current":' .. cjson.encode(cur) .. '}' end
  return '{"tag":"conflict","current":null}'
end

-- apply: mint the lease deadline from authority time inside this same atomic record, store, register gen
local stored = { rev = 'rev-' .. redis.call('INCR', rev_key), state = nx.state, config = nx.config }
if nx.inc ~= nil then stored.inc = nx.inc end
if nx.lease ~= nil then stored.lease = { id = nx.lease.id, ['until'] = now + nx.lease.durationMs } end
if nx.ttlMs ~= nil then stored.exp = now + nx.ttlMs end
local encoded = cjson.encode(stored)
redis.call('SET', head_key, encoded)
if nx.ttlMs ~= nil then redis.call('PEXPIRE', head_key, nx.ttlMs) end
if nx.inc ~= nil then redis.call('SADD', gens_key, nx.inc) end
return '{"tag":"head","head":' .. encoded .. '}'
`

// A head read and the clock used to interpret its logical tombstone are one slot-owner operation.
//   KEYS: [1]=head
export const READ_HEAD_LUA = `${HEAD_PRELUDE}
local now = tf_now()
local head = tf_read_and_expire_head(KEYS[1], now)
if not head then return '{"head":null}' end
return '{"head":' .. cjson.encode(head) .. '}'
`

// The generation's cells under a prefix, with the revision that fences reading them.
//   KEYS: [1]=head [2]=revision [3]=generation-keys
//   ARGV: [1]=inc [2]=cell-key prefix [3]=cell prefix
export const FIND_CELLS_LUA = `${HEAD_PRELUDE}
local head = tf_read_and_expire_head(KEYS[1], tf_now())
if not head or head.inc ~= ARGV[1] then return {'stale'} end
local reply = {'found', redis.call('GET', KEYS[2]) or '0'}
local wanted = ARGV[2] .. ARGV[3]
for _, key in ipairs(redis.call('SMEMBERS', KEYS[3])) do
  if string.sub(key, 1, #wanted) == wanted then reply[#reply + 1] = string.sub(key, #ARGV[2] + 1) end
end
return reply
`

// Reads cells while the head still names the incarnation (closing tails may read). With an expected
// revision, a moved one reads as 'moved'; a missing cell reads as nil.
//   KEYS: [1]=head [2]=revision [3..]=cells
//   ARGV: [1]=inc [2]=expected revision or ''
export const READ_CELLS_LUA = `${HEAD_PRELUDE}
local head = tf_read_and_expire_head(KEYS[1], tf_now())
if not head or head.inc ~= ARGV[1] then return {'stale'} end
local revision = redis.call('GET', KEYS[2]) or '0'
if ARGV[2] ~= '' and revision ~= ARGV[2] then return {'moved'} end
local reply = {'cells', revision}
for i = 3, #KEYS do reply[#reply + 1] = redis.call('GET', KEYS[i]) end
return reply
`

// Checked once SUBSCRIBE is acknowledged: a lane subscription is live only while its incarnation is the
// open head, so one closed or dropped during establishment is never reported ready.
//   KEYS: [1]=head [2]=gens
//   ARGV: [1]=inc
export const VALIDATE_GENERATION_LUA = `${HEAD_PRELUDE}
local head = tf_read_and_expire_head(KEYS[1], tf_now())
if not head or head.state ~= 'open' or head.inc ~= ARGV[1] or redis.call('SISMEMBER', KEYS[2], ARGV[1]) ~= 1 then
  return 0
end
return 1
`

// Whether the generation is still installed.
//   KEYS: [1]=gens
//   ARGV: [1]=inc
export const DROP_GENERATION_BEGIN_LUA = `
return redis.call('SISMEMBER', KEYS[1], ARGV[1])
`

// Finalize only while the generation is still installed: incarnation ids are never reused, so a
// concurrent drop that finished first leaves nothing to do. Every physical member is a declared key;
// deletion, keyed invalidation, and retirement are one atomic room-slot operation.
//   KEYS: [1]=gens [2]=invalidation-channel [3]=manifest [4..]=members
//   ARGV: [1]=inc
export const DROP_GENERATION_FINALIZE_LUA = `
local inc = ARGV[1]
if redis.call('SISMEMBER', KEYS[1], inc) == 0 then return 0 end
for i = 4, #KEYS do redis.call('UNLINK', KEYS[i]) end
redis.call('UNLINK', KEYS[3])
redis.call('PUBLISH', KEYS[2], inc)
redis.call('SREM', KEYS[1], inc)
return 1
`

// CELLS CX — all mutations or none; success implies the head precondition (open + inc) held at apply
// time; the revision is the coarse per-generation counter, allowed to over-conflict but never mislead.
//   KEYS: [1]=head [2]=rev [3]=generation-keys [4..]=cell keys (one per mutation, in order)
//   ARGV: [1]=inc [2]=expectedRev, then per mutation: op('set'|'del'), value
export const CELLS_CX_LUA = `${HEAD_PRELUDE}
local head_key, rev_key, generation_keys_key = KEYS[1], KEYS[2], KEYS[3]
local now = tf_now()
local head = tf_read_and_expire_head(head_key, now)
if (not head) or head.inc ~= ARGV[1] or head.state ~= 'open' then return 'stale-inc' end
local cur = redis.call('GET', rev_key)
if not cur then cur = '0' end
if cur ~= ARGV[2] then return 'conflict' end
local n = #KEYS - 3
for i = 1, n do
  local key = KEYS[3 + i]
  local base = 2 + (i - 1) * 2
  local op = ARGV[base + 1]
  if op == 'del' then
    redis.call('DEL', key)
    redis.call('SREM', generation_keys_key, key)
  else
    redis.call('SET', key, ARGV[base + 2])
    redis.call('SADD', generation_keys_key, key)
  end
end
redis.call('INCR', rev_key)
redis.call('SADD', generation_keys_key, rev_key)
return 'committed'
`

// COMMIT — atomic acceptance: head precondition (one boolean, two branches), order advance, optional
// retained install, then PUBLISH. Supplying a closing lease selects the narrow closing-control branch,
// which is what makes every other lane stale while closing.
//   KEYS: [1]=head [2]=order [3]=retained [4]=channel [5]=generation-keys [6..]=required live cells
//   ARGV: [1]=inc [2]=laneKind [3]=closingLease('') [4]=retain('0'|'1')
//         [5]=payload [6]=local delivery-fence token or ''
export const COMMIT_LUA = `${HEAD_PRELUDE}
${REDIS_ORDERING_FRAME_LUA}
local head_key, order_key, retained_key, channel_key, generation_keys_key =
  KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local now = tf_now()
local head = tf_read_and_expire_head(head_key, now)
local ok = false
if head and head.inc == ARGV[1] then
  if ARGV[3] == '' then
    ok = (head.state == 'open')
  else
    ok = (ARGV[2] == 'control' and head.state == 'closing' and head.lease ~= nil
          and head.lease.id == ARGV[3] and now <= head.lease['until'])
  end
end
if not ok then return '{"stale":"incarnation"}' end
for i = 6, #KEYS do
  if not redis.call('GET', KEYS[i]) then return '{"stale":"cell","index":' .. (i - 6) .. '}' end
end

-- Advance the live lane-domain cursor exactly once. It has no TTL: generation deletion is its cleanup
-- boundary. Reject safe-integer exhaustion before SET/retained/PUBLISH can have any effect.
local base_seq, base_ts = 0, 0
local prev = redis.call('GET', order_key)
if prev then
  local pseq, pts = string.match(prev, '^(%d+):(%d+)$')
  if not pseq then return redis.error_reply('commitLane: invalid ordering watermark') end
  base_seq = tonumber(pseq)
  base_ts = tonumber(pts)
end
if base_seq >= ${Number.MAX_SAFE_INTEGER} then
  return redis.error_reply('commitLane: sequence exhausted for the ordering domain')
end
local seq = base_seq + 1
local ts = now
if base_ts > ts then ts = base_ts end
-- Lua's implicit number-to-string conversion uses limited significant digits at the safe-integer
-- boundary. Format both exact integers once, then use those decimal strings for durable state and the
-- JSON receipt so the final legal commit cannot effect successfully and fail only while decoding reply.
local seq_text = string.format('%.0f', seq)
local ts_text = string.format('%.0f', ts)
redis.call('SET', order_key, seq_text .. ':' .. ts_text)
redis.call('SADD', generation_keys_key, order_key)
local frame = tf_ordering_frame(seq, ts, ARGV[5])
if ARGV[4] == '1' then
  redis.call('SET', retained_key, frame)
  redis.call('SADD', generation_keys_key, retained_key)
end
local receivers = redis.call('PUBLISH', channel_key, frame)
-- A Cluster forwards both publications from this slot owner over the same ordered bus link. The
-- impossible ordering-frame prefix makes the second publication an internal local-dispatch fence.
if ARGV[6] ~= '' then redis.call('PUBLISH', channel_key, string.char(${REDIS_DELIVERY_FENCE_BYTE}) .. ARGV[6]) end
return '{"accepted":true,"seq":' .. seq_text .. ',"timestamp":' .. ts_text .. ',"receivers":' .. receivers .. '}'
`

// Deletes one lane's retained frame, optionally only while it still holds seq ARGV[1].
//   KEYS: [1]=generation-keys [2]=retained
//   ARGV: [1]=ifSeq or ''
export const RETAINED_DELETE_LUA = `
if ARGV[1] ~= '' then
  local frame = redis.call('GET', KEYS[2])
  if not frame then return 0 end
  local seq_hi, seq_lo = struct.unpack('>I4I4', frame)
  if seq_hi * 4294967296 + seq_lo ~= tonumber(ARGV[1]) then return 0 end
end
redis.call('SREM', KEYS[1], KEYS[2])
return redis.call('DEL', KEYS[2])
`

// Directory records use two co-slotted global keys. Put and compare-delete are each one atomic record,
// so stale cleanup cannot erase (or de-index) a concurrent newer tag.
export const DIRECTORY_PUT_LUA = `
redis.call('ZADD', KEYS[1], 0, ARGV[1])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
return 1
`

export const DIRECTORY_DELETE_LUA = `
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
`

type Invocation = { keys: string[]; argv: Array<string | Buffer> }

/** One script: its KEYS and ARGV for a call, and its reply decoded. `numberOfKeys: null` sends the key count
 *  first; `binaryReply` reads bulk strings as Buffers. */
export type RedisCommand<Input, Output> = {
  readonly name: string
  readonly lua: string
  readonly numberOfKeys: number | null
  readonly binaryReply?: true
  invoke(prefix: string, input: Input): Invocation
  parse(reply: unknown, input: Input): Output
}

const command = <Input, Output>(spec: RedisCommand<Input, Output>): RedisCommand<Input, Output> => spec

type RoomInc = { roomId: string; inc: string }
type CommitInput = RoomInc & {
  lane: LaneId
  payload: Uint8Array
  retain: boolean
  closingLease: string | undefined
  requiredCellKeys: readonly string[]
  fenceToken: string
}
type CommitReply =
  | { stale: 'incarnation' }
  | { stale: 'cell'; index: number }
  | { accepted: true; seq: number; timestamp: number; receivers: number }
type CellsRead = { revision: string; cells: Map<string, Uint8Array> } | { staleInc: true }

export const REDIS_COMMANDS = {
  publish: command({
    name: 'tfPublish',
    lua: PUBLISH_LUA,
    numberOfKeys: 2,
    invoke: (prefix, { lane, payload }: { lane: BroadcastLane; payload: Uint8Array }) => ({
      keys: [broadcastSequenceKey(prefix, lane.key), broadcastChannel(prefix, lane)],
      argv: [toBuffer(payload)],
    }),
    parse: (reply) => {
      assert(
        Array.isArray(reply) && reply.length === 3 && reply.every((value) => typeof value === 'number'),
        'Publish script returned an unexpected reply',
      )
      const [seq, timestamp, receivers] = reply as [number, number, number]
      return { seq, timestamp, receivers }
    },
  }),
  headCx: command({
    name: 'tfRoomHeadCx',
    lua: HEAD_CX_LUA,
    numberOfKeys: 3,
    invoke: (prefix, { roomId, cx, next }: { roomId: string; cx: HeadCx; next: HeadNext }) => ({
      keys: [headKey(prefix, roomId), gensKey(prefix, roomId), headRevKey(prefix, roomId)],
      argv: [JSON.stringify(cx), encodeNext(next)],
    }),
    parse: (reply): HeadCxResult => {
      const parsed = JSON.parse(reply as string) as
        | { tag: 'head'; head: StoredHead }
        | { tag: 'conflict'; current: StoredHead | null }
      if (parsed.tag === 'head') return { head: toPublicHead(parsed.head) }
      return { conflict: true, current: parsed.current === null ? null : toPublicHead(parsed.current) }
    },
  }),
  readHead: command({
    name: 'tfRoomReadHead',
    lua: READ_HEAD_LUA,
    numberOfKeys: 1,
    invoke: (prefix, roomId: string) => ({ keys: [headKey(prefix, roomId)], argv: [] }),
    parse: (reply): RoomHead | null => {
      const { head } = JSON.parse(reply as string) as { head: StoredHead | null }
      return head === null ? null : toPublicHead(head)
    },
  }),
  findCells: command({
    name: 'tfRoomFindCells',
    lua: FIND_CELLS_LUA,
    numberOfKeys: 3,
    invoke: (prefix, { roomId, inc, cellPrefix }: RoomInc & { cellPrefix: string }) => ({
      keys: [headKey(prefix, roomId), revKey(prefix, roomId, inc), generationKeysKey(prefix, roomId, inc)],
      argv: [inc, cellKeyPrefix(prefix, roomId, inc), cellPrefix],
    }),
    parse: (reply): { staleInc: true } | { revision: string; keys: string[] } => {
      const [tag, revision, ...keys] = reply as string[]
      return tag === 'stale' ? { staleInc: true } : { revision: revision!, keys }
    },
  }),
  readCells: command({
    name: 'tfRoomReadCells',
    lua: READ_CELLS_LUA,
    numberOfKeys: null,
    binaryReply: true,
    invoke: (prefix, { roomId, inc, keys, revision }: RoomInc & { keys: readonly string[]; revision?: string }) => ({
      keys: [
        headKey(prefix, roomId),
        revKey(prefix, roomId, inc),
        ...keys.map((key) => cellKey(prefix, roomId, inc, key)),
      ],
      argv: [inc, revision ?? ''],
    }),
    parse: (reply, { keys }): CellsRead | 'moved' => {
      const [tag, revision, ...values] = reply as Array<Buffer | null>
      const outcome = tag!.toString()
      if (outcome === 'stale') return { staleInc: true }
      if (outcome === 'moved') return 'moved'
      const cells = new Map<string, Uint8Array>()
      keys.forEach((key, i) => {
        const value = values[i]
        if (value !== null && value !== undefined) cells.set(key, Uint8Array.from(value))
      })
      return { revision: revision!.toString(), cells }
    },
  }),
  validateGeneration: command({
    name: 'tfRoomValidateGeneration',
    lua: VALIDATE_GENERATION_LUA,
    numberOfKeys: 2,
    invoke: (prefix, { roomId, inc }: RoomInc) => ({
      keys: [headKey(prefix, roomId), gensKey(prefix, roomId)],
      argv: [inc],
    }),
    parse: (reply) => reply === 1,
  }),
  dropGenerationBegin: command({
    name: 'tfRoomDropGenerationBegin',
    lua: DROP_GENERATION_BEGIN_LUA,
    numberOfKeys: 1,
    invoke: (prefix, { roomId, inc }: RoomInc) => ({ keys: [gensKey(prefix, roomId)], argv: [inc] }),
    parse: (reply) => reply === 1,
  }),
  dropGenerationFinalize: command({
    name: 'tfRoomDropGenerationFinalize',
    lua: DROP_GENERATION_FINALIZE_LUA,
    numberOfKeys: null,
    invoke: (prefix, { roomId, inc, generationKeys }: RoomInc & { generationKeys: readonly string[] }) => ({
      keys: [
        gensKey(prefix, roomId),
        generationInvalidationChannel(prefix, roomId, inc),
        generationKeysKey(prefix, roomId, inc),
        ...generationKeys,
      ],
      argv: [inc],
    }),
    parse: () => {},
  }),
  cellsCx: command({
    name: 'tfRoomCellsCx',
    lua: CELLS_CX_LUA,
    numberOfKeys: null,
    invoke: (prefix, input: RoomInc & { revision: string; mutations: readonly CellMutation[] }) => ({
      keys: [
        headKey(prefix, input.roomId),
        revKey(prefix, input.roomId, input.inc),
        generationKeysKey(prefix, input.roomId, input.inc),
        ...input.mutations.map(({ key }) => cellKey(prefix, input.roomId, input.inc, key)),
      ],
      argv: [
        input.inc,
        input.revision,
        ...input.mutations.flatMap(({ bytes }) => (bytes === null ? ['del', ''] : ['set', toBuffer(bytes)])),
      ],
    }),
    parse: (reply) => reply as CxResult,
  }),
  commit: command({
    name: 'tfRoomCommit',
    lua: COMMIT_LUA,
    numberOfKeys: null,
    invoke: (prefix, input: CommitInput) => {
      const { roomId, inc } = input
      const laneKey = encodeLaneKey(input.lane)
      return {
        keys: [
          headKey(prefix, roomId),
          orderKey(prefix, roomId, inc, laneKey),
          retainedKey(prefix, roomId, inc, laneKey),
          channelKey(prefix, roomId, inc, laneKey),
          generationKeysKey(prefix, roomId, inc),
          ...input.requiredCellKeys.map((required) => cellKey(prefix, roomId, inc, required)),
        ],
        argv: [
          inc,
          input.lane.kind,
          input.closingLease ?? '',
          input.retain ? '1' : '0',
          toBuffer(input.payload),
          input.fenceToken,
        ],
      }
    },
    parse: (reply, { requiredCellKeys }) => {
      const parsed = JSON.parse(reply as string) as CommitReply
      if (!('stale' in parsed) || parsed.stale === 'incarnation') return parsed
      const key = requiredCellKeys[parsed.index]
      assert(key !== undefined)
      return { stale: 'cell' as const, key }
    },
  }),
  retainedDelete: command({
    name: 'tfRoomRetainedDelete',
    lua: RETAINED_DELETE_LUA,
    numberOfKeys: 2,
    invoke: (prefix, { roomId, inc, lane, ifSeq }: RoomInc & { lane: LaneId; ifSeq: number | undefined }) => ({
      keys: [generationKeysKey(prefix, roomId, inc), retainedKey(prefix, roomId, inc, encodeLaneKey(lane))],
      argv: [ifSeq === undefined ? '' : String(ifSeq)],
    }),
    parse: () => {},
  }),
  directoryPut: command({
    name: 'tfRoomDirectoryPut',
    lua: DIRECTORY_PUT_LUA,
    numberOfKeys: 2,
    invoke: (prefix, { roomId, incTag }: { roomId: string; incTag: string }) => ({
      keys: [directoryIndexKey(prefix), directoryTagsKey(prefix)],
      argv: [roomId, incTag],
    }),
    parse: () => {},
  }),
  directoryDelete: command({
    name: 'tfRoomDirectoryDelete',
    lua: DIRECTORY_DELETE_LUA,
    numberOfKeys: 2,
    invoke: (prefix, { roomId, incTag }: { roomId: string; incTag: string }) => ({
      keys: [directoryIndexKey(prefix), directoryTagsKey(prefix)],
      argv: [roomId, incTag],
    }),
    parse: () => {},
  }),
}

/** A head as the scripts store it: JSON, config base64, lease deadline minted from Redis TIME. */
type StoredHead = {
  rev: string
  state: 'open' | 'closing' | 'closed'
  config: string
  inc?: string
  lease?: { id: string; until: number }
  exp?: number
}

function toPublicHead(stored: StoredHead): RoomHead {
  const head: RoomHead = {
    rev: stored.rev,
    currentInc: stored.inc ?? null,
    state: stored.state,
    config: Uint8Array.from(Buffer.from(stored.config, 'base64')),
  }
  if (stored.lease !== undefined) head.closeLease = { id: stored.lease.id, until: stored.lease.until }
  return head
}

function encodeNext(next: HeadNext): string {
  const { head, ttlMs } = next
  const payload: Record<string, unknown> = { state: head.state, config: toBuffer(head.config).toString('base64') }
  if (head.currentInc !== null) payload.inc = head.currentInc
  if (head.closeLease !== undefined) payload.lease = { id: head.closeLease.id, durationMs: head.closeLease.durationMs }
  if (ttlMs !== undefined) payload.ttlMs = ttlMs
  return JSON.stringify(payload)
}

function toBuffer(bytes: Uint8Array): Buffer {
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength)
}
