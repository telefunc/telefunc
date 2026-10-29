export { REDIS_DELIVERY_FENCE_BYTE, REDIS_DELIVERY_FENCE_TOKEN_LENGTH, LANE_PAGE_SIZE, REDIS_COMMANDS, parseOrderMark }
export type { RedisCommand }

// Every Lua script and the one command table that registers and invokes them.

import {
  CELL_PAGE_SIZE,
  decodeLaneKey,
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
  type OrderingInfo,
  type RoomHead,
  type RoomSubscriptionSource,
  type CellsRead,
} from 'telefunc/__internal'
import { assert } from './assert.js'
import {
  broadcastChannel,
  broadcastSequenceKey,
  cellIndexKey,
  cellValuesKey,
  cellVersionsKey,
  directoryChecksKey,
  directoryIndexKey,
  directoryTagsKey,
  generationInvalidationChannel,
  generationKeysKey,
  headKey,
  memberOrderKey,
  orderKey,
  retainedKey,
  retainedLanesKey,
  retainedOfKey,
  retainedOfTrackKey,
  revKey,
  trackLanesKey,
  type CommitChannels,
} from './keys.js'

const REDIS_DELIVERY_FENCE_BYTE = 0xff
/** A fence token is a UUID. */
const REDIS_DELIVERY_FENCE_TOKEN_LENGTH = 36
/** What a start read's, a leave's or a generation drop's page holds, as a SCAN count. */
const LANE_PAGE_SIZE = 1024

// Authority time in ms from Redis TIME's [sec, µs] pair, never from the caller.
const NOW_LUA = `
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

// The same bytes as telefunc's ordering frame (wire-protocol/ordering-frame.ts): four u32 big-endian words,
// then the payload.
const REDIS_ORDERING_FRAME_LUA = `
local function tf_u64(n)
  local hi = math.floor(n / 4294967296)
  return struct.pack('>I4I4', hi, n - hi * 4294967296)
end
local function tf_ordering_frame(seq, ts, payload)
  return tf_u64(seq) .. tf_u64(ts) .. payload
end
`

// Broadcast publish: next per-key seq, authority time that never goes back on the key, one PUBLISH of the seq of the
// key's previous publish of this kind (two u32 big-endian words, 0 if none) and the ordering frame.
//   KEYS: [1]=order ('seq:ts:last text seq:last binary seq') [2]=channel
//   ARGV: [1]=payload [2]=kind ('text'|'binary')
const PUBLISH_LUA = `${NOW_LUA}${REDIS_ORDERING_FRAME_LUA}
local prev = redis.call('GET', KEYS[1])
local ts = tf_now()
local seq
local last_text, last_binary = '0', '0'
if prev then
  local pseq, pts, ptext, pbinary = string.match(prev, '^(%d+):(%d+):(%d+):(%d+)$')
  if not pseq then return redis.error_reply('publish: invalid ordering watermark') end
  seq = tonumber(pseq) + 1
  if tonumber(pts) > ts then ts = tonumber(pts) end
  last_text, last_binary = ptext, pbinary
else
  -- A missing order (new, evicted or flushed) starts at authority time in microseconds, above every seq it issued.
  local t = redis.call('TIME')
  seq = tonumber(t[1]) * 1000000 + tonumber(t[2]) + 1
end
local seq_text = string.format('%.0f', seq)
local predecessor
if ARGV[2] == 'text' then
  predecessor, last_text = tonumber(last_text), seq_text
else
  predecessor, last_binary = tonumber(last_binary), seq_text
end
redis.call('SET', KEYS[1], seq_text .. ':' .. string.format('%.0f', ts) .. ':' .. last_text .. ':' .. last_binary)
local frame = tf_u64(predecessor) .. tf_ordering_frame(seq, ts, ARGV[1])
local receivers = redis.call('PUBLISH', KEYS[2], frame)
return {seq, ts, receivers}
`

// HEAD CX: compares by form, then stores the next head.
//   KEYS: [1]=head
//   ARGV: [1]=HeadCx JSON {form,rev?,lease?} [2]=nextJson{state,inc?,config,lease?,ttlMs?}
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
if nx.ttlMs ~= nil then stored.exp = now + nx.ttlMs end
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
// incarnation (closing tails may read), after the authority time of the read. A missing cell is left out.
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
local now = tf_now()
local head = tf_live_head(KEYS[1], now)
if not head or head.inc ~= ARGV[1] then return {'stale'} end
local keys, first = ARGV, 3
if ARGV[2] == 'prefix' then
  keys, first = redis.call('ZRANGEBYLEX', KEYS[2], '[' .. ARGV[3], '(' .. ARGV[3] .. '\\255'), 1
end
return tf_cells_reply({'cells', string.format('%.0f', now)}, keys, first)
`

// A prefix read's page: up to ARGV[4] cells of the index range from ARGV[2] ('[' then the prefix, or '(' then the
// previous page's last key) to past the prefix ARGV[3], after the authority time of the read. A full page is tagged
// 'more', with its last key after the time.
//   KEYS: [1]=head [2]=cell-index [3]=values [4]=versions
//   ARGV: [1]=inc [2]=range start [3]=prefix [4]=page size
const READ_CELL_PAGE_LUA = `${HEAD_PRELUDE}${CELLS_REPLY_LUA}
local now = tf_now()
local head = tf_live_head(KEYS[1], now)
if not head or head.inc ~= ARGV[1] then return {'stale'} end
local limit = tonumber(ARGV[4])
local keys = redis.call('ZRANGEBYLEX', KEYS[2], ARGV[2], '(' .. ARGV[3] .. '\\255', 'LIMIT', 0, limit)
local time = string.format('%.0f', now)
if #keys < limit then return tf_cells_reply({'cells', time}, keys, 1) end
return tf_cells_reply({'more', time, keys[#keys]}, keys, 1)
`

// Read once SUBSCRIBE is acknowledged: a lane subscription is live only while its incarnation is the open head, so one
// closed or dropped during establishment is never reported ready, and its frames start after its lane's last commit.
//   KEYS: [1]=head [2]=order: an inbox or binary lane's member's order hash, or another lane's own key
//   ARGV: [1]=inc [2]=an inbox or binary lane's key, or ''
//   Reply: nil once the incarnation is not the open head, else the lane's 'seq:ts', '' before its first commit
const LANE_START_LUA = `${HEAD_PRELUDE}
local head = tf_live_head(KEYS[1], tf_now())
if not head or head.state ~= 'open' or head.inc ~= ARGV[1] then return false end
local mark
if ARGV[2] == '' then mark = redis.call('GET', KEYS[2]) else mark = redis.call('HGET', KEYS[2], ARGV[2]) end
return mark or ''
`

// A page of a member's lanes' last commits, read as LANE_START_LUA reads one lane's, for its every-track subscription.
//   KEYS: [1]=head [2]=the member's order hash
//   ARGV: [1]=inc [2]=HSCAN cursor
//   Reply: nil once the incarnation is not the open head, else {next cursor, {lane key, 'seq:ts', ...}}
const MEMBER_LANE_STARTS_LUA = `${HEAD_PRELUDE}
local head = tf_live_head(KEYS[1], tf_now())
if not head or head.state ~= 'open' or head.inc ~= ARGV[1] then return false end
return redis.call('HSCAN', KEYS[2], ARGV[2], 'COUNT', ${LANE_PAGE_SIZE})
`

// A page of a track's binary lanes that have a commit, read as LANE_START_LUA reads one lane,
// for its every-member subscription.
//   KEYS: [1]=head [2]=the track's lane set
//   ARGV: [1]=inc [2]=SSCAN cursor
//   Reply: nil once the incarnation is not the open head, else {next cursor, {lane key, ...}}
const TRACK_LANES_LUA = `${HEAD_PRELUDE}
local head = tf_live_head(KEYS[1], tf_now())
if not head or head.state ~= 'open' or head.inc ~= ARGV[1] then return false end
return redis.call('SSCAN', KEYS[2], ARGV[2], 'COUNT', ${LANE_PAGE_SIZE})
`

// Each binary lane's last commit, '' for one whose member's order is gone.
//   KEYS: [1..]=each lane's member's order hash
//   ARGV: [1..]=the lane keys, in KEYS's order
const LANE_MARKS_LUA = `
local marks = {}
for i = 1, #KEYS do marks[i] = redis.call('HGET', KEYS[i], ARGV[i]) or '' end
return marks
`

// Read once SUBSCRIBE is acknowledged: a key's frames start after its last publish. A key with none starts at authority
// time in microseconds, as PUBLISH_LUA would start it, so its first publish follows.
//   KEYS: [1]=order ('seq:ts:last text seq:last binary seq')
const BROADCAST_START_LUA = `
local mark = redis.call('GET', KEYS[1])
if mark then return mark end
local t = redis.call('TIME')
mark = string.format('%.0f', tonumber(t[1]) * 1000000 + tonumber(t[2])) .. ':0:0:0'
redis.call('SET', KEYS[1], mark)
return mark
`

// Drops the manifest once its keys are; incarnation ids are never reused, so a repeated drop deletes nothing.
//   KEYS: [1]=invalidation-channel [2]=manifest
//   ARGV: [1]=inc
const DROP_GENERATION_LUA = `
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
// published on its member's every-track channel and its track's every-member channel, after its lane key's two-byte
// length and the key, so a receiver orders it by its own lane; no lane key is long enough for that length's first byte
// to read as the fence byte.
//   KEYS: [1]=head [2]=order: an inbox or binary lane's member's order hash, which holds it under its lane key, or
//         another lane's own key [3]=retained [4]=channel [5]=generation-keys [6]=cell values
//         [7]=a binary lane's member's every-track channel (another lane repeats [4])
//         [8]=a retained binary lane's member's retained lanes [9]=its track's retained lanes (another commit repeats
//         [3] for both) [10]=a binary lane's track's every-member channel (another lane repeats [4]) [11]=a binary
//         lane's track's lane set, which its first commit enters (another lane repeats [2]) [12]=the generation's
//         retained lanes
//   ARGV: [1]=inc [2]=laneKind [3]=closingLease('') [4]=retain('0'|'1') [5]=payload
//         [6]=local delivery-fence token for [4] or '' [7]=the same for [7] or '' [8]=lane key [9]=the same as [6] for
//         [10] or '' [10..]=required live cells
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
for i = 10, #ARGV do
  if redis.call('HEXISTS', KEYS[6], ARGV[i]) == 0 then return '{"stale":"cell","index":' .. (i - 10) .. '}' end
end

local member_lane = ARGV[2] == 'inbox' or ARGV[2] == 'binary'
local base_seq, base_ts = 0, 0
local prev
if member_lane then prev = redis.call('HGET', order_key, ARGV[8]) else prev = redis.call('GET', order_key) end
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
if member_lane then
  redis.call('HSET', order_key, ARGV[8], seq_text .. ':' .. ts_text)
else
  redis.call('SET', order_key, seq_text .. ':' .. ts_text)
end
redis.call('SADD', generation_keys_key, order_key)
if not prev and ARGV[2] == 'binary' then
  redis.call('SADD', KEYS[11], ARGV[8])
  redis.call('SADD', generation_keys_key, KEYS[11])
end
local frame = tf_ordering_frame(seq, ts, ARGV[5])
if ARGV[4] == '1' then
  -- A lane holding a retained frame is already indexed: its frame and index entries leave together.
  if redis.call('EXISTS', retained_key) == 0 then
    redis.call('SADD', generation_keys_key, retained_key, KEYS[12])
    redis.call('SADD', KEYS[12], ARGV[8])
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
  local fan_in = struct.pack('>I2', #ARGV[8]) .. ARGV[8] .. frame
  local every_track, every_member = fan_in, fan_in
  if ARGV[7] ~= '' then every_track = fence .. ARGV[7] .. fan_in end
  if ARGV[9] ~= '' then every_member = fence .. ARGV[9] .. fan_in end
  receivers = receivers + redis.call('PUBLISH', KEYS[7], every_track) + redis.call('PUBLISH', KEYS[10], every_member)
end
return '{"accepted":true,"seq":' .. seq_text .. ',"timestamp":' .. ts_text .. ',"receivers":' .. receivers .. '}'
`

// Deletes one lane's retained frame, optionally only while it still holds seq ARGV[1].
//   KEYS: [1]=generation-keys [2]=retained [3]=a binary lane's member's retained lanes [4]=its track's retained lanes
//         (another lane repeats [2] for both) [5]=the generation's retained lanes
//   ARGV: [1]=ifSeq or '' [2]=lane kind [3]=lane key
const RETAINED_DELETE_LUA = `
if ARGV[1] ~= '' then
  local frame = redis.call('GET', KEYS[2])
  if not frame then return 0 end
  local seq_hi, seq_lo = struct.unpack('>I4I4', frame)
  if seq_hi * 4294967296 + seq_lo ~= tonumber(ARGV[1]) then return 0 end
end
redis.call('SREM', KEYS[1], KEYS[2])
redis.call('SREM', KEYS[5], ARGV[3])
if ARGV[2] == 'binary' then
  redis.call('SREM', KEYS[3], ARGV[3])
  if redis.call('SCARD', KEYS[3]) == 0 then redis.call('SREM', KEYS[1], KEYS[3]) end
  redis.call('SREM', KEYS[4], ARGV[3])
  if redis.call('SCARD', KEYS[4]) == 0 then redis.call('SREM', KEYS[1], KEYS[4]) end
end
return redis.call('DEL', KEYS[2])
`

// Takes a page of a leaving member's binary lanes out of their tracks' lane sets.
//   KEYS: [1]=generation-keys [2..]=each lane's track's lane set
//   ARGV: [1..]=the lane keys, in KEYS[2..]'s order
const FORGET_TRACK_LANES_LUA = `
for i = 2, #KEYS do
  redis.call('SREM', KEYS[i], ARGV[i - 1])
  if redis.call('SCARD', KEYS[i]) == 0 then redis.call('SREM', KEYS[1], KEYS[i]) end
end
`

// Forgets one member's lane order.
//   KEYS: [1]=generation-keys [2]=the member's order hash
const DROP_LANE_ORDERS_LUA = `
redis.call('SREM', KEYS[1], KEYS[2])
redis.call('UNLINK', KEYS[2])
`

// Directory records use three co-slotted global keys. Put and compare-delete are each one atomic record,
// so stale cleanup cannot erase (or de-index) a concurrent newer tag.
//   KEYS: [1]=index [2]=tags [3]=checks
//   ARGV: [1]=room id [2]=tag [3]=when it is due, '' for never [4]=the least delay from authority time (put only)
const DIRECTORY_PUT_LUA = `${NOW_LUA}
redis.call('ZADD', KEYS[1], 0, ARGV[1])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
if ARGV[3] == '' then
  redis.call('ZREM', KEYS[3], ARGV[1])
else
  local at = math.max(tonumber(ARGV[3]), tf_now() + tonumber(ARGV[4]))
  redis.call('ZADD', KEYS[3], string.format('%.0f', at), ARGV[1])
end
return 1
`

const DIRECTORY_DELETE_LUA = `
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
return 1
`

// Up to ARGV[1] entries due at authority time, earliest first: each room id, then its tag.
//   KEYS: [1]=checks [2]=tags
const DIRECTORY_DUE_LUA = `${NOW_LUA}
local reply = {}
local now = string.format('%.0f', tf_now())
for _, roomId in ipairs(redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))) do
  reply[#reply + 1] = roomId
  reply[#reply + 1] = redis.call('HGET', KEYS[2], roomId)
end
return reply
`

type Invocation = { keys: readonly string[]; argv: ReadonlyArray<string | Buffer> }

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
type DirectoryPutInput = { roomId: string; incTag: string; checkAt: number; minDelayMs: number }

type CommitInput = RoomInc & {
  lane: LaneId
  laneKey: string
  channels: CommitChannels
  payload: Uint8Array
  retain: boolean
  closingLease: string | undefined
  requiredCellKeys: readonly string[]
  /** Each channel's fence token, `''` where no local attempt awaits the commit. */
  fenceTokens: { lane: string; everyTrack: string; everyMember: string }
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
      argv: [typeof payload === 'string' ? payload : toBuffer(payload), route.kind],
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
      const [tag, now, ...found] = reply as Buffer[]
      return tag!.toString() === 'stale' ? { staleInc: true } : parseCells(now!, found)
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
      const [tag, now, ...found] = reply as Buffer[]
      if (tag!.toString() === 'stale') return { staleInc: true }
      if (tag!.toString() === 'cells') return parseCells(now!, found)
      return { ...parseCells(now!, found.slice(1)), after: found[0]!.toString() }
    },
  }),
  laneStart: command({
    name: 'tfRoomLaneStart',
    lua: LANE_START_LUA,
    numberOfKeys: 2,
    invoke: (prefix, { roomId, inc, lane }: RoomSubscriptionSource) => {
      const laneKey = encodeLaneKey(lane)
      const memberLane = lane.kind === 'inbox' || lane.kind === 'binary'
      return {
        keys: [
          headKey(prefix, roomId),
          memberLane ? memberOrderKey(prefix, roomId, inc, lane.member) : orderKey(prefix, roomId, inc, laneKey),
        ],
        argv: [inc, memberLane ? laneKey : ''],
      }
    },
    parse: (reply): Map<string, OrderingInfo> | null => {
      if (reply === null) return null
      return reply === '' ? new Map() : new Map([['', parseOrderMark(reply as string)]])
    },
  }),
  memberLaneStarts: command({
    name: 'tfRoomMemberLaneStarts',
    lua: MEMBER_LANE_STARTS_LUA,
    numberOfKeys: 2,
    invoke: (prefix, { roomId, inc, member, cursor }: RoomInc & { member: string; cursor: string }) => ({
      keys: [headKey(prefix, roomId), memberOrderKey(prefix, roomId, inc, member)],
      argv: [inc, cursor],
    }),
    parse: (reply): { cursor: string; starts: Array<[string, OrderingInfo]> } | null => {
      if (reply === null) return null
      const [cursor, fields] = reply as [string, string[]]
      const starts: Array<[string, OrderingInfo]> = []
      for (let i = 0; i < fields.length; i += 2) starts.push([fields[i]!, parseOrderMark(fields[i + 1]!)])
      return { cursor, starts }
    },
  }),
  trackLanes: command({
    name: 'tfRoomTrackLanes',
    lua: TRACK_LANES_LUA,
    numberOfKeys: 2,
    invoke: (prefix, { roomId, inc, track, cursor }: RoomInc & { track: string; cursor: string }) => ({
      keys: [headKey(prefix, roomId), trackLanesKey(prefix, roomId, inc, track)],
      argv: [inc, cursor],
    }),
    parse: (reply): { cursor: string; lanes: string[] } | null => {
      if (reply === null) return null
      const [cursor, lanes] = reply as [string, string[]]
      return { cursor, lanes }
    },
  }),
  laneMarks: command({
    name: 'tfRoomLaneMarks',
    lua: LANE_MARKS_LUA,
    numberOfKeys: null,
    invoke: (prefix, { roomId, inc, lanes }: RoomInc & { lanes: readonly string[] }) => ({
      keys: lanes.map((laneKey) => memberOrderKey(prefix, roomId, inc, binaryLaneOf(laneKey).member)),
      argv: lanes,
    }),
    parse: (reply) => (reply as string[]).map((mark) => (mark === '' ? null : parseOrderMark(mark))),
  }),
  broadcastStart: command({
    name: 'tfBroadcastStart',
    lua: BROADCAST_START_LUA,
    numberOfKeys: 1,
    invoke: (prefix, route: BroadcastRoute) => ({ keys: [broadcastSequenceKey(prefix, route.key)], argv: [] }),
    parse: (reply) => new Map([['', parseOrderMark(reply as string)]]),
  }),
  dropGeneration: command({
    name: 'tfRoomDropGeneration',
    lua: DROP_GENERATION_LUA,
    numberOfKeys: 2,
    invoke: (prefix, { roomId, inc }: RoomInc) => ({
      keys: [generationInvalidationChannel(prefix, roomId, inc), generationKeysKey(prefix, roomId, inc)],
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
      for (let i = 0; i < counted.length; i += 3) {
        counters.set(counted[i]!, { value: Number(counted[i + 1]), changedAt: Number(counted[i + 2]) })
      }
      return { committed: counters }
    },
  }),
  commit: command({
    name: 'tfRoomCommit',
    lua: COMMIT_LUA,
    numberOfKeys: 12,
    invoke: (prefix, input: CommitInput) => {
      const { roomId, inc, lane, laneKey, channels } = input
      const retained = retainedKey(prefix, roomId, inc, laneKey)
      const indexed = lane.kind === 'binary' && input.retain
      const order =
        lane.kind === 'inbox' || lane.kind === 'binary'
          ? memberOrderKey(prefix, roomId, inc, lane.member)
          : orderKey(prefix, roomId, inc, laneKey)
      return {
        keys: [
          headKey(prefix, roomId),
          order,
          retained,
          channels.lane,
          generationKeysKey(prefix, roomId, inc),
          cellValuesKey(prefix, roomId, inc),
          channels.everyTrack ?? channels.lane,
          indexed ? retainedOfKey(prefix, roomId, inc, lane.member) : retained,
          indexed ? retainedOfTrackKey(prefix, roomId, inc, lane.track) : retained,
          channels.everyMember ?? channels.lane,
          lane.kind === 'binary' ? trackLanesKey(prefix, roomId, inc, lane.track) : order,
          retainedLanesKey(prefix, roomId, inc),
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
          input.fenceTokens.everyMember,
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
    numberOfKeys: 5,
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
          retainedLanesKey(prefix, roomId, inc),
        ],
        argv: [ifSeq === undefined ? '' : String(ifSeq), lane.kind, laneKey],
      }
    },
    parse: () => {},
  }),
  forgetTrackLanes: command({
    name: 'tfRoomForgetTrackLanes',
    lua: FORGET_TRACK_LANES_LUA,
    numberOfKeys: null,
    invoke: (prefix, { roomId, inc, lanes }: RoomInc & { lanes: readonly string[] }) => ({
      keys: [
        generationKeysKey(prefix, roomId, inc),
        ...lanes.map((laneKey) => trackLanesKey(prefix, roomId, inc, binaryLaneOf(laneKey).track)),
      ],
      argv: lanes,
    }),
    parse: () => {},
  }),
  dropLaneOrders: command({
    name: 'tfRoomDropLaneOrders',
    lua: DROP_LANE_ORDERS_LUA,
    numberOfKeys: 2,
    invoke: (prefix, { roomId, inc, member }: RoomInc & { member: string }) => ({
      keys: [generationKeysKey(prefix, roomId, inc), memberOrderKey(prefix, roomId, inc, member)],
      argv: [],
    }),
    parse: () => {},
  }),
  directoryPut: command({
    name: 'tfRoomDirectoryPut',
    lua: DIRECTORY_PUT_LUA,
    numberOfKeys: 3,
    invoke: (prefix, { roomId, incTag, checkAt, minDelayMs }: DirectoryPutInput) => ({
      keys: [directoryIndexKey(prefix), directoryTagsKey(prefix), directoryChecksKey(prefix)],
      argv: [roomId, incTag, checkAt === Infinity ? '' : String(checkAt), String(minDelayMs)],
    }),
    parse: () => {},
  }),
  directoryDelete: command({
    name: 'tfRoomDirectoryDelete',
    lua: DIRECTORY_DELETE_LUA,
    numberOfKeys: 3,
    invoke: (prefix, { roomId, incTag }: { roomId: string; incTag: string }) => ({
      keys: [directoryIndexKey(prefix), directoryTagsKey(prefix), directoryChecksKey(prefix)],
      argv: [roomId, incTag],
    }),
    parse: () => {},
  }),
  directoryDue: command({
    name: 'tfRoomDirectoryDue',
    lua: DIRECTORY_DUE_LUA,
    numberOfKeys: 2,
    invoke: (prefix, { limit }: { limit: number }) => ({
      keys: [directoryChecksKey(prefix), directoryTagsKey(prefix)],
      argv: [String(limit)],
    }),
    parse: (reply) => {
      const found = reply as string[]
      const entries: { roomId: string; incTag: string }[] = []
      for (let i = 0; i < found.length; i += 2) entries.push({ roomId: found[i]!, incTag: found[i + 1]! })
      return entries
    },
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

/** The read's time, then key, value and version triples. */
function parseCells(
  now: Buffer,
  found: Buffer[],
): { cells: Map<string, Uint8Array>; versions: Map<string, string>; now: number } {
  const cells = new Map<string, Uint8Array>()
  const versions = new Map<string, string>()
  for (let i = 0; i < found.length; i += 3) {
    const key = found[i]!.toString()
    cells.set(key, Uint8Array.from(found[i + 1]!))
    versions.set(key, found[i + 2]!.toString())
  }
  return { cells, versions, now: Number(now.toString()) }
}

/** A copy, as ioredis may hold a command past the call (connecting, auto-pipelining, a NOSCRIPT or TRYAGAIN retry). */
function toBuffer(bytes: Uint8Array): Buffer {
  return Buffer.from(bytes)
}

function parseOrderMark(mark: string): OrderingInfo {
  const [seq, timestamp] = mark.split(':').map(Number) as [number, number]
  return { seq, timestamp }
}

function binaryLaneOf(laneKey: string): Extract<LaneId, { kind: 'binary' }> {
  const lane = decodeLaneKey(laneKey)
  assert(lane.kind === 'binary')
  return lane
}
