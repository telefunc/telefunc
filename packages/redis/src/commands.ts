export { REDIS_DELIVERY_FENCE_BYTE, REDIS_DELIVERY_FENCE_TOKEN_LENGTH, REDIS_COMMANDS }
export type { RedisCommand }

// Every Lua script and the one command table that registers and invokes them.

import {
  encodeLaneKey,
  type BroadcastPayload,
  type BroadcastRoute,
  type CellCounter,
  type CellSelector,
  type CellsCx,
  type CxResult,
  type HeadCx,
  type HeadCxResult,
  type HeadNext,
  type LaneId,
  type RoomHead,
  type CellsRead,
} from 'telefunc/__internal'
import { assert } from './assert.js'
import {
  broadcastChannel,
  broadcastSequenceKey,
  cellIndexKey,
  cellValuesKey,
  cellVersionsKey,
  directoryIndexKey,
  directoryTagsKey,
  generationInvalidationChannel,
  generationKeysKey,
  headKey,
  orderKey,
  retainedKey,
  retainedOfKey,
  retainedOfTrackKey,
  revKey,
  type CommitChannels,
} from './keys.js'

const REDIS_DELIVERY_FENCE_BYTE = 0xff
/** A fence token is a UUID. */
const REDIS_DELIVERY_FENCE_TOKEN_LENGTH = 36
/** The cells a prefix read's page holds. */
const CELL_PAGE_SIZE = 1024

// Authority time in ms from Redis TIME's [sec, µs] pair, never from the caller.
// Redis 4 refuses a script's write after TIME unless the script replicates its effects, the default since Redis 5.
const NOW_LUA = `
redis.replicate_commands()
local function tf_now()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
`

const HEAD_PRELUDE = `${NOW_LUA}
-- read the head, treating a logically-lapsed head as absent (a lapsed tombstone reopens the
-- absence epoch); the PX backstop only reclaims memory, it is never what makes it invisible.
local function tf_live_head(key, now)
  local raw = redis.call('GET', key)
  if not raw then return nil end
  local h = cjson.decode(raw)
  if h.exp and h.exp <= now then return nil end
  return h
end
`

// The same bytes as telefunc's ordering frame (wire-protocol/ordering-frame.ts): four u32 big-endian words, then the payload.
const REDIS_ORDERING_FRAME_LUA = `
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
const PUBLISH_LUA = `${NOW_LUA}${REDIS_ORDERING_FRAME_LUA}
-- A missing counter (new, evicted or flushed) starts at authority time in microseconds, above every seq it issued.
if redis.call('EXISTS', KEYS[1]) == 0 then
  local t = redis.call('TIME')
  redis.call('SET', KEYS[1], string.format('%d', tonumber(t[1]) * 1000000 + tonumber(t[2])))
end
local seq = redis.call('INCR', KEYS[1])
local ts = tf_now()
local frame = tf_ordering_frame(seq, ts, ARGV[1])
local receivers = redis.call('PUBLISH', KEYS[2], frame)
return {seq, ts, receivers}
`

// HEAD CX: compares by form, then stores the next head.
//   KEYS: [1]=head
//   ARGV: [1]=HeadCx JSON {form,rev?,lease?} [2]=nextJson{state,inc?,config,lease?,ttlMs? (ms or 'keep')}
const HEAD_CX_LUA = `${HEAD_PRELUDE}
local head_key = KEYS[1]
local now = tf_now()
local cx = cjson.decode(ARGV[1])
local nx = cjson.decode(ARGV[2])
local cur = tf_live_head(head_key, now)

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

-- The count lapses with a tombstone; salted with authority time, a rev still never repeats.
local n = (cur and cur.n or 0) + 1
local stored = { rev = 'rev-' .. string.format('%d', now) .. '-' .. n, n = n, state = nx.state, config = nx.config }
if nx.inc ~= nil then stored.inc = nx.inc end
if nx.lease ~= nil then stored.lease = { id = nx.lease.id, ['until'] = now + nx.lease.durationMs } end
if nx.ttlMs == 'keep' then
  if cur ~= nil then stored.exp = cur.exp end
elseif nx.ttlMs ~= nil then
  stored.exp = now + nx.ttlMs
end
local encoded = cjson.encode(stored)
redis.call('SET', head_key, encoded)
if stored.exp ~= nil then redis.call('PEXPIREAT', head_key, stored.exp) end
return '{"tag":"head","head":' .. encoded .. '}'
`

// A head read and the clock used to interpret its logical tombstone are one slot-owner operation.
//   KEYS: [1]=head
const READ_HEAD_LUA = `${HEAD_PRELUDE}
local now = tf_now()
local head = tf_live_head(KEYS[1], now)
if not head then return '{"head":null}' end
return '{"head":' .. cjson.encode(head) .. '}'
`

// Reads the named cells, or every cell under a prefix, each with its version, while the head still names the
// incarnation (closing tails may read). A missing cell is left out.
//   KEYS: [1]=head [2]=cell-index [3]=values [4]=versions
//   ARGV: [1]=inc [2]='keys' then the cell keys, or 'prefix' then the prefix
// No UTF-8 text holds the byte 0xFF, so a prefix's range ends past every key under it.
// Appends each of keys[first..] that has a value, with its value and version, to reply.
const CELLS_REPLY_LUA = `
local function tf_cells_reply(reply, keys, first)
  for i = first, #keys do
    local value = redis.call('HGET', KEYS[3], keys[i])
    if value then
      reply[#reply + 1] = keys[i]
      reply[#reply + 1] = value
      reply[#reply + 1] = redis.call('HGET', KEYS[4], keys[i])
    end
  end
  return reply
end
`

const READ_CELLS_LUA = `${HEAD_PRELUDE}${CELLS_REPLY_LUA}
local head = tf_live_head(KEYS[1], tf_now())
if not head or head.inc ~= ARGV[1] then return {'stale'} end
local keys, first = ARGV, 3
if ARGV[2] == 'prefix' then
  keys, first = redis.call('ZRANGEBYLEX', KEYS[2], '[' .. ARGV[3], '(' .. ARGV[3] .. '\\255'), 1
end
return tf_cells_reply({'cells'}, keys, first)
`

// A prefix read's page: up to ARGV[4] cells of the index range from ARGV[2] ('[' then the prefix, or '(' then the
// previous page's last key) to past the prefix ARGV[3]. A full page is tagged 'more' with its last key.
//   KEYS: [1]=head [2]=cell-index [3]=values [4]=versions
//   ARGV: [1]=inc [2]=range start [3]=prefix [4]=page size
const READ_CELL_PAGE_LUA = `${HEAD_PRELUDE}${CELLS_REPLY_LUA}
local head = tf_live_head(KEYS[1], tf_now())
if not head or head.inc ~= ARGV[1] then return {'stale'} end
local limit = tonumber(ARGV[4])
local keys = redis.call('ZRANGEBYLEX', KEYS[2], ARGV[2], '(' .. ARGV[3] .. '\\255', 'LIMIT', 0, limit)
if #keys < limit then return tf_cells_reply({'cells'}, keys, 1) end
return tf_cells_reply({'more', keys[#keys]}, keys, 1)
`

// Checked once SUBSCRIBE is acknowledged: a lane subscription is live only while its incarnation is the
// open head, so one closed or dropped during establishment is never reported ready.
//   KEYS: [1]=head
//   ARGV: [1]=inc
const VALIDATE_GENERATION_LUA = `${HEAD_PRELUDE}
local head = tf_live_head(KEYS[1], tf_now())
if not head or head.state ~= 'open' or head.inc ~= ARGV[1] then
  return 0
end
return 1
`

// One atomic room-slot operation; incarnation ids are never reused, so a repeated drop deletes nothing.
//   KEYS: [1]=invalidation-channel [2]=manifest [3..]=members
//   ARGV: [1]=inc
const DROP_GENERATION_LUA = `
for i = 3, #KEYS do redis.call('UNLINK', KEYS[i]) end
redis.call('UNLINK', KEYS[2])
redis.call('PUBLISH', KEYS[1], ARGV[1])
`

// CELLS CX: all mutations and counts or none, while each expected cell is at its version; success implies the head
// precondition (open + inc) held at apply time. A commit's writes take the generation's next revision as version.
//   KEYS: [1]=head [2]=rev [3]=generation-keys [4]=cell-index [5]=values [6]=versions
//   ARGV: [1]=inc [2..4]=how many mutated, expected and counted cells, then per mutated cell its key, op('set'|'del')
//         and value, per expected cell its key and version ('' while absent), and per counted cell its key, delta and
//         create ('1' or '')
const CELLS_CX_LUA = `${HEAD_PRELUDE}
local head_key, rev_key, generation_keys_key, cell_index_key, values_key, versions_key =
  KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]
local now = tf_now()
local head = tf_live_head(head_key, now)
if (not head) or head.inc ~= ARGV[1] or head.state ~= 'open' then return {'stale-inc'} end
local mutated, expected, counted = tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local first_expected = 5 + 3 * mutated
local first_counted = first_expected + 2 * expected
for at = first_expected, first_counted - 1, 2 do
  if (redis.call('HGET', versions_key, ARGV[at]) or '') ~= ARGV[at + 1] then return {'conflict'} end
end
local version = redis.call('INCR', rev_key)
local function write(key, value)
  if value then
    redis.call('HSET', values_key, key, value)
    redis.call('HSET', versions_key, key, version)
    redis.call('ZADD', cell_index_key, 0, key)
  else
    redis.call('HDEL', values_key, key)
    redis.call('HDEL', versions_key, key)
    redis.call('ZREM', cell_index_key, key)
  end
end
for at = 5, first_expected - 1, 3 do
  write(ARGV[at], ARGV[at + 1] == 'set' and ARGV[at + 2] or nil)
end
local reply = {'committed'}
for at = first_counted, first_counted + 3 * counted - 1, 3 do
  local key, delta = ARGV[at], tonumber(ARGV[at + 1])
  local raw = redis.call('HGET', values_key, key)
  local counter = raw and cjson.decode(raw)
  if not counter and ARGV[at + 2] == '1' then counter = { value = 0 } end
  if counter then
    if delta ~= 0 or not raw then
      counter.value = counter.value + delta
      counter.changedAt = now
      write(key, string.format('{"value":%d,"changedAt":%d}', counter.value, counter.changedAt))
    end
    reply[#reply + 1] = key
    reply[#reply + 1] = string.format('%d', counter.value)
    reply[#reply + 1] = string.format('%d', counter.changedAt)
  end
end
redis.call('SADD', generation_keys_key, rev_key, cell_index_key, values_key, versions_key)
return reply
`

// COMMIT: while the room is closing, only the close's control commit under its lease lands. A binary commit is also
// published on its member's every-track channel, after its lane key's two-byte length and the key, so a receiver orders
// it by its own lane; no lane key is long enough for that length's first byte to read as the fence byte.
//   KEYS: [1]=head [2]=order [3]=retained [4]=channel [5]=generation-keys [6]=cell values
//         [7]=a binary lane's member's every-track channel (another lane repeats [4])
//         [8]=a retained binary lane's member's retained lanes [9]=its track's retained lanes (another commit repeats
//         [3] for both)
//   ARGV: [1]=inc [2]=laneKind [3]=closingLease('') [4]=retain('0'|'1') [5]=payload
//         [6]=local delivery-fence token for [4] or '' [7]=the same for [7] or '' [8]=lane key [9..]=required live cells
const COMMIT_LUA = `${HEAD_PRELUDE}
${REDIS_ORDERING_FRAME_LUA}
local head_key, order_key, retained_key, channel_key, generation_keys_key =
  KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local now = tf_now()
local head = tf_live_head(head_key, now)
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
for i = 9, #ARGV do
  if redis.call('HEXISTS', KEYS[6], ARGV[i]) == 0 then return '{"stale":"cell","index":' .. (i - 9) .. '}' end
end

local base_seq, base_ts = 0, 0
local prev = redis.call('GET', order_key)
if prev then
  local pseq, pts = string.match(prev, '^(%d+):(%d+)$')
  if not pseq then return redis.error_reply('commitLane: invalid ordering watermark') end
  base_seq = tonumber(pseq)
  base_ts = tonumber(pts)
end
local seq = base_seq + 1
local ts = now
if base_ts > ts then ts = base_ts end
-- tostring keeps only 14 significant digits.
local seq_text = string.format('%.0f', seq)
local ts_text = string.format('%.0f', ts)
redis.call('SET', order_key, seq_text .. ':' .. ts_text)
redis.call('SADD', generation_keys_key, order_key)
local frame = tf_ordering_frame(seq, ts, ARGV[5])
if ARGV[4] == '1' then
  -- A lane holding a retained frame is already indexed: its frame and index entries leave together.
  if redis.call('EXISTS', retained_key) == 0 then
    redis.call('SADD', generation_keys_key, retained_key)
    if ARGV[2] == 'binary' then
      redis.call('SADD', KEYS[8], ARGV[8])
      redis.call('SADD', KEYS[9], ARGV[8])
      redis.call('SADD', generation_keys_key, KEYS[8], KEYS[9])
    end
  end
  redis.call('SET', retained_key, frame)
end
-- A channel's local attempts awaiting the commit find their fence token ahead of its frame, after a byte no frame
-- starts with.
local fence = string.char(${REDIS_DELIVERY_FENCE_BYTE})
local lane_message = frame
if ARGV[6] ~= '' then lane_message = fence .. ARGV[6] .. frame end
local receivers = redis.call('PUBLISH', channel_key, lane_message)
if ARGV[2] == 'binary' then
  local every_track_prefix = struct.pack('>I2', #ARGV[8]) .. ARGV[8]
  if ARGV[7] ~= '' then every_track_prefix = fence .. ARGV[7] .. every_track_prefix end
  receivers = receivers + redis.call('PUBLISH', KEYS[7], every_track_prefix .. frame)
end
return '{"accepted":true,"seq":' .. seq_text .. ',"timestamp":' .. ts_text .. ',"receivers":' .. receivers .. '}'
`

// Deletes one lane's retained frame, optionally only while it still holds seq ARGV[1].
//   KEYS: [1]=generation-keys [2]=retained [3]=a binary lane's member's retained lanes [4]=its track's retained lanes
//         (another lane repeats [2] for both)
//   ARGV: [1]=ifSeq or '' [2]=a binary lane's key or ''
const RETAINED_DELETE_LUA = `
if ARGV[1] ~= '' then
  local frame = redis.call('GET', KEYS[2])
  if not frame then return 0 end
  local seq_hi, seq_lo = struct.unpack('>I4I4', frame)
  if seq_hi * 4294967296 + seq_lo ~= tonumber(ARGV[1]) then return 0 end
end
redis.call('SREM', KEYS[1], KEYS[2])
if ARGV[2] ~= '' then
  redis.call('SREM', KEYS[3], ARGV[2])
  if redis.call('SCARD', KEYS[3]) == 0 then redis.call('SREM', KEYS[1], KEYS[3]) end
  redis.call('SREM', KEYS[4], ARGV[2])
  if redis.call('SCARD', KEYS[4]) == 0 then redis.call('SREM', KEYS[1], KEYS[4]) end
end
return redis.call('DEL', KEYS[2])
`

// Directory records use two co-slotted global keys. Put and compare-delete are each one atomic record,
// so stale cleanup cannot erase (or de-index) a concurrent newer tag.
const DIRECTORY_PUT_LUA = `
redis.call('ZADD', KEYS[1], 0, ARGV[1])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
return 1
`

const DIRECTORY_DELETE_LUA = `
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
`

type Invocation = { keys: string[]; argv: Array<string | Buffer> }

/** One script: its KEYS and ARGV for a call, and its reply decoded. `numberOfKeys: null` sends the key count
 *  first; `binaryReply` reads bulk strings as Buffers. */
type RedisCommand<Input, Output> = {
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
  laneKey: string
  channels: CommitChannels
  payload: Uint8Array
  retain: boolean
  closingLease: string | undefined
  requiredCellKeys: readonly string[]
  /** Each channel's fence token, `''` where no local attempt awaits the commit. */
  fenceTokens: { lane: string; everyTrack: string }
}
type CommitReply =
  | { stale: 'incarnation' }
  | { stale: 'cell'; index: number }
  | { accepted: true; seq: number; timestamp: number; receivers: number }

const REDIS_COMMANDS = {
  publish: command({
    name: 'tfPublish',
    lua: PUBLISH_LUA,
    numberOfKeys: 2,
    invoke: (prefix, { route, payload }: { route: BroadcastRoute; payload: BroadcastPayload }) => ({
      keys: [broadcastSequenceKey(prefix, route.key), broadcastChannel(prefix, route)],
      // ioredis sends a string as UTF-8.
      argv: [typeof payload === 'string' ? payload : toBuffer(payload)],
    }),
    parse: (reply) => {
      // A shared client with ioredis's `stringNumbers` returns integer replies as decimal strings.
      const values = Array.isArray(reply) ? reply.map(Number) : []
      assert(values.length === 3 && values.every(Number.isSafeInteger), 'Publish script returned an unexpected reply')
      const [seq, timestamp, receivers] = values as [number, number, number]
      return { seq, timestamp, receivers }
    },
  }),
  headCx: command({
    name: 'tfRoomHeadCx',
    lua: HEAD_CX_LUA,
    numberOfKeys: 1,
    invoke: (prefix, { roomId, cx, next }: { roomId: string; cx: HeadCx; next: HeadNext }) => ({
      keys: [headKey(prefix, roomId)],
      argv: [JSON.stringify(cx), encodeNext(next)],
    }),
    parse: (reply): HeadCxResult => {
      const parsed = JSON.parse(reply as string) as
        | { tag: 'head'; head: ScriptHead }
        | { tag: 'conflict'; current: ScriptHead | null }
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
      const { head } = JSON.parse(reply as string) as { head: ScriptHead | null }
      return head === null ? null : toPublicHead(head)
    },
  }),
  readCells: command({
    name: 'tfRoomReadCells',
    lua: READ_CELLS_LUA,
    numberOfKeys: 4,
    binaryReply: true,
    invoke: (prefix, { roomId, inc, sel }: RoomInc & { sel: CellSelector }) => ({
      keys: [
        headKey(prefix, roomId),
        cellIndexKey(prefix, roomId, inc),
        cellValuesKey(prefix, roomId, inc),
        cellVersionsKey(prefix, roomId, inc),
      ],
      argv: [inc, ...('keys' in sel ? ['keys', ...sel.keys] : ['prefix', sel.prefix])],
    }),
    parse: (reply): CellsRead => {
      const [tag, ...found] = reply as Buffer[]
      return tag!.toString() === 'stale' ? { staleInc: true } : parseCells(found)
    },
  }),
  readCellPage: command({
    name: 'tfRoomReadCellPage',
    lua: READ_CELL_PAGE_LUA,
    numberOfKeys: 4,
    binaryReply: true,
    invoke: (prefix, { roomId, inc, cellPrefix, after }: RoomInc & { cellPrefix: string; after?: string }) => ({
      keys: [
        headKey(prefix, roomId),
        cellIndexKey(prefix, roomId, inc),
        cellValuesKey(prefix, roomId, inc),
        cellVersionsKey(prefix, roomId, inc),
      ],
      argv: [inc, after === undefined ? `[${cellPrefix}` : `(${after}`, cellPrefix, String(CELL_PAGE_SIZE)],
    }),
    parse: (reply): CellsRead & { after?: string } => {
      const [tag, ...found] = reply as Buffer[]
      if (tag!.toString() === 'stale') return { staleInc: true }
      if (tag!.toString() === 'cells') return parseCells(found)
      return { ...parseCells(found.slice(1)), after: found[0]!.toString() }
    },
  }),
  validateGeneration: command({
    name: 'tfRoomValidateGeneration',
    lua: VALIDATE_GENERATION_LUA,
    numberOfKeys: 1,
    invoke: (prefix, { roomId, inc }: RoomInc) => ({
      keys: [headKey(prefix, roomId)],
      argv: [inc],
    }),
    parse: (reply) => Number(reply) === 1,
  }),
  dropGeneration: command({
    name: 'tfRoomDropGeneration',
    lua: DROP_GENERATION_LUA,
    numberOfKeys: null,
    invoke: (prefix, { roomId, inc, generationKeys }: RoomInc & { generationKeys: readonly string[] }) => ({
      keys: [
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
    numberOfKeys: 6,
    invoke: (prefix, { roomId, inc, cx }: RoomInc & { cx: CellsCx }) => {
      const { counts = [] } = cx
      return {
        keys: [
          headKey(prefix, roomId),
          revKey(prefix, roomId, inc),
          generationKeysKey(prefix, roomId, inc),
          cellIndexKey(prefix, roomId, inc),
          cellValuesKey(prefix, roomId, inc),
          cellVersionsKey(prefix, roomId, inc),
        ],
        argv: [
          inc,
          String(cx.mutations.length),
          String(cx.expect.length),
          String(counts.length),
          ...cx.mutations.flatMap(({ key, bytes }) =>
            bytes === null ? [key, 'del', ''] : [key, 'set', toBuffer(bytes)],
          ),
          ...cx.expect.flatMap(({ key, version }) => [key, version ?? '']),
          ...counts.flatMap(({ key, delta, create }) => [key, String(delta), create ? '1' : '']),
        ],
      }
    },
    parse: (reply): CxResult => {
      const [tag, ...counted] = reply as string[]
      if (tag !== 'committed') return tag as 'conflict' | 'stale-inc'
      const counters = new Map<string, CellCounter>()
      for (let i = 0; i < counted.length; i += 3)
        counters.set(counted[i]!, { value: Number(counted[i + 1]), changedAt: Number(counted[i + 2]) })
      return { committed: counters }
    },
  }),
  commit: command({
    name: 'tfRoomCommit',
    lua: COMMIT_LUA,
    numberOfKeys: 9,
    invoke: (prefix, input: CommitInput) => {
      const { roomId, inc, lane, laneKey, channels } = input
      const retained = retainedKey(prefix, roomId, inc, laneKey)
      const indexed = lane.kind === 'binary' && input.retain
      return {
        keys: [
          headKey(prefix, roomId),
          orderKey(prefix, roomId, inc, laneKey),
          retained,
          channels.lane,
          generationKeysKey(prefix, roomId, inc),
          cellValuesKey(prefix, roomId, inc),
          channels.everyTrack ?? channels.lane,
          indexed ? retainedOfKey(prefix, roomId, inc, lane.member) : retained,
          indexed ? retainedOfTrackKey(prefix, roomId, inc, lane.track) : retained,
        ],
        argv: [
          inc,
          lane.kind,
          input.closingLease ?? '',
          input.retain ? '1' : '0',
          toBuffer(input.payload),
          input.fenceTokens.lane,
          input.fenceTokens.everyTrack,
          laneKey,
          ...input.requiredCellKeys,
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
    numberOfKeys: 4,
    invoke: (prefix, { roomId, inc, lane, ifSeq }: RoomInc & { lane: LaneId; ifSeq: number | undefined }) => {
      const laneKey = encodeLaneKey(lane)
      const retained = retainedKey(prefix, roomId, inc, laneKey)
      const binary = lane.kind === 'binary'
      return {
        keys: [
          generationKeysKey(prefix, roomId, inc),
          retained,
          binary ? retainedOfKey(prefix, roomId, inc, lane.member) : retained,
          binary ? retainedOfTrackKey(prefix, roomId, inc, lane.track) : retained,
        ],
        argv: [ifSeq === undefined ? '' : String(ifSeq), binary ? laneKey : ''],
      }
    },
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
type ScriptHead = {
  rev: string
  n: number
  state: 'open' | 'closing' | 'closed'
  config: string
  inc?: string
  lease?: { id: string; until: number }
  exp?: number
}

function toPublicHead(stored: ScriptHead): RoomHead {
  const head: RoomHead = {
    rev: stored.rev,
    currentInc: stored.inc ?? null,
    state: stored.state,
    config: Uint8Array.from(Buffer.from(stored.config, 'base64')),
  }
  if (stored.lease !== undefined) head.closeLease = { id: stored.lease.id, until: stored.lease.until }
  if (stored.exp !== undefined) head.expiresAt = stored.exp
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

// Key, value and version triples.
function parseCells(found: Buffer[]): { cells: Map<string, Uint8Array>; versions: Map<string, string> } {
  const cells = new Map<string, Uint8Array>()
  const versions = new Map<string, string>()
  for (let i = 0; i < found.length; i += 3) {
    const key = found[i]!.toString()
    cells.set(key, Uint8Array.from(found[i + 1]!))
    versions.set(key, found[i + 2]!.toString())
  }
  return { cells, versions }
}

function toBuffer(bytes: Uint8Array): Buffer {
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength)
}
