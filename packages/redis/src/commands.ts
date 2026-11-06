export { REDIS_DELIVERY_FENCE_BYTE, REDIS_DELIVERY_FENCE_TOKEN_LENGTH, LANE_PAGE_SIZE, REDIS_COMMANDS, parseOrdering }
export type { CommitInput, OperationOutcome, RedisCommand }

// Every Lua script and the one command table that registers and invokes them.

import type { BroadcastPayload, BroadcastRoute, OrderingInfo } from 'telefunc/__internal'
import { ChannelOverflowError } from 'telefunc/__internal'
import type {
  CellCounter,
  CellSelector,
  CellsExchange,
  CellsExchangeResult,
  HeadExchangeResult,
  HeadExpectation,
  HeadNext,
  LaneId,
  OperationWrites,
  RoomHead,
  RoomSubscriptionSource,
  CellsRead,
  ExpiringCellsRead,
  RoomPlaneKit,
  StaleCommit,
  StaleIncarnation,
} from '@telefunc/room/backend'
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
  expiringCellKey,
  incarnationInvalidationChannel,
  incarnationKeysKey,
  headKey,
  memberOrderKey,
  orderKey,
  retainedKey,
  retainedLanesKey,
  retainedOwnersKey,
  retainedOfKey,
  retainedOfTrackKey,
  revKey,
  trackLanesKey,
  type CommitChannels,
  type FenceTokens,
} from './keys.js'

const REDIS_DELIVERY_FENCE_BYTE = 0xff
/** A fence token is a UUID. */
const REDIS_DELIVERY_FENCE_TOKEN_LENGTH = 36
/** What a start read's, a leave's or an incarnation drop's page holds, as a SCAN count. */
const LANE_PAGE_SIZE = 1024
/** Over this, Redis closes the subscriber: its allocator rounds the buffer up to the default pubsub limit, 32 MiB. */
const PUBLISH_MAX_BYTES = 28 * 1024 * 1024

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

// The same bytes as telefunc's ordering frame (wire-protocol/wire/orderingFrame.ts): four u32 big-endian words,
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

// Broadcast publish: next per-key seq, authority time that never goes back on the key, one PUBLISH of this publish's
// place among the key's publishes of its kind (two u32 big-endian words, from 1) and the ordering frame.
//   KEYS: [1]=order ('seq:ts:text publishes:binary publishes') [2]=channel
//   ARGV: [1]=payload [2]=kind ('text'|'binary')
const PUBLISH_LUA = `${NOW_LUA}${REDIS_ORDERING_FRAME_LUA}
local prev = redis.call('GET', KEYS[1])
local ts = tf_now()
local seq
local text, binary = 0, 0
if prev then
  local pseq, pts, ptext, pbinary = string.match(prev, '^(%d+):(%d+):(%d+):(%d+)$')
  if not pseq then return redis.error_reply('publish: invalid ordering watermark') end
  seq = tonumber(pseq) + 1
  if tonumber(pts) > ts then ts = tonumber(pts) end
  text, binary = tonumber(ptext), tonumber(pbinary)
else
  -- A missing order (new, evicted or flushed) starts at authority time in microseconds, above every seq it issued.
  local t = redis.call('TIME')
  seq = tonumber(t[1]) * 1000000 + tonumber(t[2]) + 1
end
local place
if ARGV[2] == 'text' then
  text = text + 1
  place = text
else
  binary = binary + 1
  place = binary
end
redis.call('SET', KEYS[1], string.format('%.0f:%.0f:%.0f:%.0f', seq, ts, text, binary))
local frame = tf_u64(place) .. tf_ordering_frame(seq, ts, ARGV[1])
redis.call('PUBLISH', KEYS[2], frame)
return {seq, ts}
`

// Writes a cell at `version`, or deletes it with no value; counts a counter cell, appending its key, value and
// changedAt to `reply` (see CellCount).
//   KEYS: [4]=cell-index [5]=values [6]=versions, as every script that uses them has them
const CELL_WRITES_LUA = `
local function tf_write(key, value, version)
  if value then
    redis.call('HSET', KEYS[5], key, value)
    redis.call('HSET', KEYS[6], key, version)
    redis.call('ZADD', KEYS[4], 0, key)
  else
    redis.call('HDEL', KEYS[5], key)
    redis.call('HDEL', KEYS[6], key)
    redis.call('ZREM', KEYS[4], key)
  end
end
local function tf_count(key, delta, create, version, now, reply)
  local raw = redis.call('HGET', KEYS[5], key)
  local counter = raw and cjson.decode(raw)
  if not counter and create then counter = { value = 0 } end
  if not counter then return end
  if delta ~= 0 or not raw then
    counter.value = counter.value + delta
    counter.changedAt = now
    tf_write(key, string.format('{"value":%d,"changedAt":%d}', counter.value, counter.changedAt), version)
  end
  reply[#reply + 1] = key
  reply[#reply + 1] = string.format('%d', counter.value)
  reply[#reply + 1] = string.format('%d', counter.changedAt)
end
`

// HEAD EXCHANGE: compares by form, then stores the next head, and counts a create's cells in its incarnation.
//   KEYS: [1]=head, and with counts [2]=rev [3]=incarnation-keys [4]=cell-index [5]=values [6]=versions of the next
//         head's incarnation
//   ARGV: [1]=HeadExpectation JSON {form,rev?,lease?} [2]=nextJson{state,inc?,config,lease?,ttlMs?} [3..]=per counted
//         cell its key, delta and create ('1' or '')
const HEAD_EXCHANGE_LUA = `${HEAD_PRELUDE}${CELL_WRITES_LUA}
local head_key = KEYS[1]
local now = tf_now()
local expected = cjson.decode(ARGV[1])
local nx = cjson.decode(ARGV[2])
local cur = tf_live_head(head_key, now)

local matches = false
if expected.form == 'absent' then
  matches = (cur == nil)
elseif cur ~= nil and cur.rev == expected.rev then
  if expected.form == 'takeover' then
    matches = (cur.state == 'closing' and cur.lease ~= nil and cur.lease['until'] < now)
  elseif expected.form == 'finalize' then
    matches = (cur.state == 'closing' and cur.lease ~= nil and cur.lease.id == expected.lease)
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
if #KEYS > 1 then
  local version = redis.call('INCR', KEYS[2])
  for at = 3, #ARGV, 3 do tf_count(ARGV[at], tonumber(ARGV[at + 1]), ARGV[at + 2] == '1', version, now, {}) end
  redis.call('SADD', KEYS[3], KEYS[2], KEYS[4], KEYS[5], KEYS[6])
end
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
if not head or head.inc ~= ARGV[1] then return {'stale-incarnation'} end
local keys, first = ARGV, 3
if ARGV[2] == 'prefix' then
  keys, first = redis.call('ZRANGEBYLEX', KEYS[2], '[' .. ARGV[3], '(' .. ARGV[3] .. '\\255'), 1
end
return tf_cells_reply({'cells', string.format('%.0f', now)}, keys, first)
`

// The expiring cells of the keys that Redis hasn't expired yet, after the head names the incarnation.
//   KEYS: [1]=head, then per cell its expiring key
//   ARGV: [1]=inc, then per cell its key
const READ_EXPIRING_CELLS_LUA = `${HEAD_PRELUDE}
local head = tf_live_head(KEYS[1], tf_now())
if not head or head.inc ~= ARGV[1] then return {'stale-incarnation'} end
local reply = {'cells'}
for i = 2, #KEYS do
  local bytes = redis.call('GET', KEYS[i])
  if bytes then
    reply[#reply + 1] = ARGV[i]
    reply[#reply + 1] = bytes
  end
end
return reply
`

// A prefix read's page: up to ARGV[4] cells of the index range from ARGV[2] ('[' then the prefix, or '(' then the
// previous page's last key) to past the prefix ARGV[3], after the authority time of the read. A full page is tagged
// 'more', with its last key after the time.
//   KEYS: [1]=head [2]=cell-index [3]=values [4]=versions
//   ARGV: [1]=inc [2]=range start [3]=prefix [4]=page size
const READ_CELL_PAGE_LUA = `${HEAD_PRELUDE}${CELLS_REPLY_LUA}
local now = tf_now()
local head = tf_live_head(KEYS[1], now)
if not head or head.inc ~= ARGV[1] then return {'stale-incarnation'} end
local limit = tonumber(ARGV[4])
local keys = redis.call('ZRANGEBYLEX', KEYS[2], ARGV[2], '(' .. ARGV[3] .. '\\255', 'LIMIT', 0, limit)
local time = string.format('%.0f', now)
if #keys < limit then return tf_cells_reply({'cells', time}, keys, 1) end
return tf_cells_reply({'more', time, keys[#keys]}, keys, 1)
`

// Read once SUBSCRIBE is acknowledged: a lane subscription is live only while its incarnation is the open head, so one
// closed or dropped during establishment is never reported ready, and its frames start after its lane's last commit.
//   KEYS: [1]=head [2]=order: the order hash of an inbox or binary lane's member, or another lane's own key
//   ARGV: [1]=inc [2]=an inbox or binary lane's key, or ''
//   Reply: nil once the incarnation is not the open head, else the lane's 'seq:ts', '' before its first commit
const LANE_START_LUA = `${HEAD_PRELUDE}
local head = tf_live_head(KEYS[1], tf_now())
if not head or head.state ~= 'open' or head.inc ~= ARGV[1] then return false end
local mark
if ARGV[2] == '' then mark = redis.call('GET', KEYS[2]) else mark = redis.call('HGET', KEYS[2], ARGV[2]) end
return mark or ''
`

// A page of a member's lanes' last commits, inbox included, read as LANE_START_LUA reads one lane's, for its every-track
// subscription.
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
//   KEYS: [1]=order ('seq:ts:text publishes:binary publishes')
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
const DROP_INCARNATION_LUA = `
redis.call('UNLINK', KEYS[2])
redis.call('PUBLISH', KEYS[1], ARGV[1])
`

// CELLS EXCHANGE: all mutations and counts or none, while each expected cell is at its version; success implies the
// head precondition (open + inc) held at apply time. A commit's writes take the incarnation's next revision as version.
//   KEYS: [1]=head [2]=rev [3]=incarnation-keys [4]=cell-index [5]=values [6]=versions
//   ARGV: [1]=inc [2..4]=how many mutated, expected and counted cells, then per mutated cell its key, op('set'|'del')
//         and value, per expected cell its key and version ('' while absent), and per counted cell its key, delta and
//         create ('1' or '')
const CELLS_EXCHANGE_LUA = `${HEAD_PRELUDE}${CELL_WRITES_LUA}
local head_key, rev_key, incarnation_keys_key, cell_index_key, values_key, versions_key =
  KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]
local now = tf_now()
local head = tf_live_head(head_key, now)
if (not head) or head.inc ~= ARGV[1] or head.state ~= 'open' then return {'stale-incarnation'} end
local mutated, expected, counted = tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local first_expected = 5 + 3 * mutated
local first_counted = first_expected + 2 * expected
for at = first_expected, first_counted - 1, 2 do
  if (redis.call('HGET', versions_key, ARGV[at]) or '') ~= ARGV[at + 1] then return {'conflict'} end
end
local version = redis.call('INCR', rev_key)
for at = 5, first_expected - 1, 3 do
  tf_write(ARGV[at], ARGV[at + 1] == 'set' and ARGV[at + 2] or nil, version)
end
local reply = {'committed'}
for at = first_counted, first_counted + 3 * counted - 1, 3 do
  tf_count(ARGV[at], tonumber(ARGV[at + 1]), ARGV[at + 2] == '1', version, now, reply)
end
redis.call('SADD', incarnation_keys_key, rev_key, cell_index_key, values_key, versions_key)
return reply
`

// COMMIT: while the room is closing, only the close's control commit under its lease lands. A binary commit is also
// published on the every-track channel of its member and the every-member channel of its track, after its lane key's
// two-byte length and the key, so a receiver orders it by its own lane; no lane key is long enough for that length's
// first byte to read as the fence byte.

/** COMMIT's KEYS, by index: the script reads each by its name, and `commit.invoke` writes each there. A key the lane
 *  has none of repeats another: outside a binary lane, `everyTrackChannel` and `everyMemberChannel` repeat
 *  `channel` and `trackLanes` repeats `order`; for an unindexed retain, `memberRetained` and `trackRetained` repeat
 *  `retained`. */
const COMMIT_KEYS = {
  head: 0,
  /** The order hash of an inbox or binary lane's member, which holds the lane under its lane key; for any other lane,
   *  the lane's own key. */
  order: 1,
  retained: 2,
  channel: 3,
  incarnationKeys: 4,
  cellValues: 5,
  everyTrackChannel: 6,
  /** A retained binary lane's member's retained lanes. */
  memberRetained: 7,
  /** A retained binary lane's track's retained lanes. */
  trackRetained: 8,
  everyMemberChannel: 9,
  /** A binary lane's track's lane set, which its first commit enters. */
  trackLanes: 10,
  /** The incarnation's retained lanes. */
  retainedLanes: 11,
  /** The member each retained frame of a lane other than a binary one is from. */
  retainedOwners: 12,
} as const

/** COMMIT's ARGV, by index; the required live cells follow the last. A fence token is the channel's local
 *  delivery-fence token, or `''`. */
const COMMIT_ARGS = {
  inc: 0,
  laneKind: 1,
  /** `''` unless the close's control commit. */
  closingLease: 2,
  /** `'0'` or `'1'`. */
  retain: 3,
  payload: 4,
  channelFence: 5,
  everyTrackFence: 6,
  laneKey: 7,
  everyMemberFence: 8,
  /** The member a retained frame is from, `''` for none. */
  owner: 9,
  requiredCells: 10,
} as const

/** Each name of `table` as the script reads it from the table `array` names: index 0 is `KEYS[1]` or `ARGV[1]`.
 *  Built once, at module load. */
function luaRefs<Name extends string>(array: string, table: Record<Name, number>): Record<Name, string> {
  const refs = {} as Record<Name, string>
  for (const name in table) refs[name] = `${array}[${table[name] + 1}]`
  return refs
}

const COMMIT_KEY = luaRefs('KEYS', COMMIT_KEYS)
const COMMIT_ARG = luaRefs('ARGV', COMMIT_ARGS)

/** COMMIT's body, reading its keys from `K` and its args from `A`, a table `args` names. */
function commitLua(
  K: Record<keyof typeof COMMIT_KEYS, string>,
  A: Record<keyof typeof COMMIT_ARGS, string>,
  args: string,
) {
  return `local head_key, order_key, retained_key, channel_key, incarnation_keys_key =
  ${K.head}, ${K.order}, ${K.retained}, ${K.channel}, ${K.incarnationKeys}
local now = tf_now()
local head = tf_live_head(head_key, now)
local ok = false
if head and head.inc == ${A.inc} then
  if ${A.closingLease} == '' then
    ok = (head.state == 'open')
  else
    ok = (${A.laneKind} == 'control' and head.state == 'closing' and head.lease ~= nil
          and head.lease.id == ${A.closingLease} and now <= head.lease['until'])
  end
end
if not ok then return '{"kind":"stale-incarnation"}' end
for i = ${COMMIT_ARGS.requiredCells + 1}, #${args} do
  if redis.call('HEXISTS', ${K.cellValues}, ${args}[i]) == 0 then
    return '{"kind":"stale-cell","index":' .. (i - ${COMMIT_ARGS.requiredCells + 1}) .. '}'
  end
end

local member_lane = ${A.laneKind} == 'inbox' or ${A.laneKind} == 'binary'
local base_seq, base_ts = 0, 0
local prev
if member_lane then prev = redis.call('HGET', order_key, ${A.laneKey}) else prev = redis.call('GET', order_key) end
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
  redis.call('HSET', order_key, ${A.laneKey}, seq_text .. ':' .. ts_text)
else
  redis.call('SET', order_key, seq_text .. ':' .. ts_text)
end
redis.call('SADD', incarnation_keys_key, order_key)
if not prev and ${A.laneKind} == 'binary' then
  redis.call('SADD', ${K.trackLanes}, ${A.laneKey})
  redis.call('SADD', incarnation_keys_key, ${K.trackLanes})
end
local frame = tf_ordering_frame(seq, ts, ${A.payload})
if ${A.retain} == '1' then
  -- A lane holding a retained frame is already indexed: its frame and index entries leave together.
  if redis.call('EXISTS', retained_key) == 0 then
    redis.call('SADD', incarnation_keys_key, retained_key, ${K.retainedLanes})
    redis.call('SADD', ${K.retainedLanes}, ${A.laneKey})
    if ${A.laneKind} == 'binary' then
      redis.call('SADD', ${K.memberRetained}, ${A.laneKey})
      redis.call('SADD', ${K.trackRetained}, ${A.laneKey})
      redis.call('SADD', incarnation_keys_key, ${K.memberRetained}, ${K.trackRetained})
    end
  end
  redis.call('SET', retained_key, frame)
  if ${A.laneKind} ~= 'binary' then
    if ${A.owner} == '' then
      redis.call('HDEL', ${K.retainedOwners}, ${A.laneKey})
    else
      redis.call('HSET', ${K.retainedOwners}, ${A.laneKey}, ${A.owner})
      redis.call('SADD', incarnation_keys_key, ${K.retainedOwners})
    end
  end
end
-- A channel's local subscriptions awaiting the commit find their fence token ahead of its frame, after a byte no frame
-- starts with.
local fence = string.char(${REDIS_DELIVERY_FENCE_BYTE})
local lane_message = frame
if ${A.channelFence} ~= '' then lane_message = fence .. ${A.channelFence} .. frame end
redis.call('PUBLISH', channel_key, lane_message)
if ${A.laneKind} == 'binary' then
  local fan_in = struct.pack('>I2', #${A.laneKey}) .. ${A.laneKey} .. frame
  local every_track, every_member = fan_in, fan_in
  if ${A.everyTrackFence} ~= '' then every_track = fence .. ${A.everyTrackFence} .. fan_in end
  if ${A.everyMemberFence} ~= '' then every_member = fence .. ${A.everyMemberFence} .. fan_in end
  redis.call('PUBLISH', ${K.everyTrackChannel}, every_track)
  redis.call('PUBLISH', ${K.everyMemberChannel}, every_member)
end
return '{"kind":"accepted","seq":' .. seq_text .. ',"timestamp":' .. ts_text .. '}'
`
}

const COMMIT_LUA = `${HEAD_PRELUDE}
${REDIS_ORDERING_FRAME_LUA}
${commitLua(COMMIT_KEY, COMMIT_ARG, 'ARGV')}`

// Deletes one lane's retained frame. `member` and `track` are the retained lanes of a binary lane's member and its
// track; another lane repeats `retained` for both, and its owner leaves `owners`.
const RETAINED_DELETE_FN_LUA = `
local function tf_retained_delete(incarnation_keys, all, owners, retained, member, track, kind, lane_key)
  redis.call('SREM', incarnation_keys, retained)
  redis.call('SREM', all, lane_key)
  if kind == 'binary' then
    redis.call('SREM', member, lane_key)
    if redis.call('SCARD', member) == 0 then redis.call('SREM', incarnation_keys, member) end
    redis.call('SREM', track, lane_key)
    if redis.call('SCARD', track) == 0 then redis.call('SREM', incarnation_keys, track) end
  else
    redis.call('HDEL', owners, lane_key)
  end
  return redis.call('DEL', retained)
end
`

// OPERATION: an operation's writes, while the head names its incarnation open, every cell its plan read is still at
// the version it read, and each member it drops has the lanes and retained binary frames the executor read: the prefix
// drops, the mutations and counts at the incarnation's next revision, the members' lane orders, their retained binary
// frames and the semantic frame of each that owns it as the script finds it, the track lane sets of the members'
// binary lanes, then its commit, as COMMIT commits.
//   KEYS: `OPERATION_KEYS`, then the keys the plan gives the index of
//   ARGV: [1]=inc [2]=the plan, an `OperationPacket` as JSON (its type says what each field holds and where its KEYS
//         and ARGV are), then the bytes and COMMIT's ARGV the plan gives the index of
const OPERATION_KEYS = {
  head: 0,
  rev: 1,
  incarnationKeys: 2,
  // Where `CELL_WRITES_LUA` reads them.
  cellIndex: 3,
  cellValues: 4,
  cellVersions: 5,
  retainedLanes: 6,
  /** The semantic lane's retained frame. */
  semanticRetained: 7,
  retainedOwners: 8,
} as const
const OPERATION_KEY = luaRefs('KEYS', OPERATION_KEYS)

const OPERATION_LUA = `${HEAD_PRELUDE}${REDIS_ORDERING_FRAME_LUA}${CELL_WRITES_LUA}${RETAINED_DELETE_FN_LUA}
local function tf_commit(CK, CA)
${commitLua(luaRefs('CK', COMMIT_KEYS), luaRefs('CA', COMMIT_ARGS), 'CA')}
end
local rev_key, incarnation_keys_key = ${OPERATION_KEY.rev}, ${OPERATION_KEY.incarnationKeys}
local cell_index_key, retained_lanes_key = ${OPERATION_KEY.cellIndex}, ${OPERATION_KEY.retainedLanes}
local semantic_key, owners_key = ${OPERATION_KEY.semanticRetained}, ${OPERATION_KEY.retainedOwners}
local now = tf_now()
local head = tf_live_head(${OPERATION_KEY.head}, now)
if (not head) or head.inc ~= ARGV[1] or head.state ~= 'open' then return '{"kind":"stale-incarnation"}' end
local plan = cjson.decode(ARGV[2])
for _, read in ipairs(plan.expect) do
  if (redis.call('HGET', ${OPERATION_KEY.cellVersions}, read[1]) or '') ~= read[2] then return '{"kind":"conflict"}' end
end
for _, member in ipairs(plan.members) do
  if redis.call('HLEN', KEYS[member[4]]) ~= member[2] or redis.call('SCARD', KEYS[member[4] + 1]) ~= member[3] then
    return '{"kind":"conflict"}'
  end
end
for _, prefix in ipairs(plan.prefixes) do
  for _, key in ipairs(redis.call('ZRANGEBYLEX', cell_index_key, '[' .. prefix, '(' .. prefix .. '\\255')) do
    tf_write(key, nil, nil)
  end
end
local version = redis.call('INCR', rev_key)
for _, mutation in ipairs(plan.mutations) do
  tf_write(mutation[1], mutation[2] > 0 and ARGV[mutation[2]] or nil, version)
end
local counters = {}
for _, count in ipairs(plan.counts) do tf_count(count[1], count[2], count[3], version, now, counters) end
redis.call('SADD', incarnation_keys_key, rev_key, cell_index_key, ${OPERATION_KEY.cellValues},
  ${OPERATION_KEY.cellVersions})
for _, member in ipairs(plan.members) do
  redis.call('SREM', incarnation_keys_key, KEYS[member[4]])
  redis.call('UNLINK', KEYS[member[4]])
  if redis.call('HGET', owners_key, plan.semantic) == member[1] then
    tf_retained_delete(incarnation_keys_key, retained_lanes_key, owners_key, semantic_key, semantic_key, semantic_key,
      'semantic', plan.semantic)
  end
end
for _, lane in ipairs(plan.retained) do
  local at = lane[2]
  tf_retained_delete(incarnation_keys_key, retained_lanes_key, owners_key, KEYS[at], KEYS[at + 1], KEYS[at + 2],
    'binary', lane[1])
end
for _, lane in ipairs(plan.trackLanes) do
  redis.call('SREM', KEYS[lane[2]], lane[1])
  if redis.call('SCARD', KEYS[lane[2]]) == 0 then redis.call('SREM', incarnation_keys_key, KEYS[lane[2]]) end
end
for _, cell in ipairs(plan.expiring) do redis.call('SET', KEYS[cell[3]], ARGV[cell[1]], 'PX', cell[2]) end
local commit = 'null'
if plan.commit[1] then
  local CK, CA = {}, {}
  for i = 1, ${Object.keys(COMMIT_KEYS).length} do CK[i] = KEYS[plan.commit[1] + i - 1] end
  for i = plan.commit[2], #ARGV do CA[#CA + 1] = ARGV[i] end
  commit = tf_commit(CK, CA)
end
return {commit, unpack(counters)}
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
  owner: string | undefined
  closingLease: string | undefined
  requiredCellKeys: readonly string[]
  fenceTokens: FenceTokens
}

/** The commit script's JSON reply: a missing required cell by its index among them. */
type CommitReply =
  | StaleIncarnation
  | { kind: 'stale-cell'; index: number }
  | { kind: 'accepted'; seq: number; timestamp: number }

/** A commit as the driver hands it on: a missing required cell by its key. */
type CommitOutcome = StaleCommit | Extract<CommitReply, { kind: 'accepted' }>

/** OPERATION_LUA's ARGV[2], as JSON: what the plan read and wrote. A `keys` number is where the entry's KEYS start, and
 *  `invoke` alone decides the order of the KEYS past the script's fixed ones. */
type OperationPacket = {
  /** Each cell the plan read, with its version as read ('' while absent): the script applies nothing once one moved. */
  expect: [key: string, version: string][]
  /** Each member the plan drops, with its lane orders and retained lanes counted as read; its order hash and retained
   *  lanes are its KEYS. */
  members: [member: string, lanes: number, retained: number, keys: number][]
  /** The semantic lane's key, whose retained frame goes with a dropped member that owns it. */
  semantic: string
  /** The cell prefixes the plan drops, before its mutations. */
  prefixes: string[]
  /** Each cell write: its key, and its bytes' index in ARGV, 0 for a delete. */
  mutations: [key: string, argv: number][]
  /** Each counter cell's change (`CellCount`). */
  counts: [key: string, delta: number, create: boolean][]
  /** The dropped members' retained binary lanes; a frame's key and its member's and track's retained lanes are its
   *  KEYS. */
  retained: [lane: string, keys: number][]
  /** The dropped members' binary lanes, which leave their tracks' lane sets, their KEY. */
  trackLanes: [lane: string, keys: number][]
  /** Each expiring cell: its bytes' index in ARGV, its TTL, and its KEY. */
  expiring: [argv: number, ttlMs: number, keys: number][]
  /** Where COMMIT's KEYS and ARGV start; empty without a commit. */
  commit: [keys: number, argv: number] | []
}

/** What OPERATION applies: an operation's writes, the version each cell its plan read was at ('' while absent), each
 *  member it drops with the lanes its order hash had and the binary lanes it retained, and its commit. */
type OperationInput = RoomInc & {
  writes: OperationWrites
  reads: ReadonlyMap<string, string>
  members: readonly { member: string; lanes: readonly string[]; retained: readonly string[] }[]
  commit: CommitInput | null
  kit: RoomPlaneKit
}

type OperationOutcome =
  | StaleIncarnation
  | { kind: 'conflict' }
  | { kind: 'done'; counters: Map<string, CellCounter>; commit: CommitReply | null }

const REDIS_COMMANDS = {
  publish: command({
    name: 'tfPublish',
    lua: PUBLISH_LUA,
    numberOfKeys: 2,
    invoke: (prefix, { route, payload }: { route: BroadcastRoute; payload: BroadcastPayload }) => {
      assertPublishable(typeof payload === 'string' ? Buffer.byteLength(payload) : payload.byteLength)
      return {
        keys: [broadcastSequenceKey(prefix, route.key), broadcastChannel(prefix, route)],
        // ioredis sends a string as UTF-8.
        argv: [typeof payload === 'string' ? payload : toBuffer(payload), route.kind],
      }
    },
    parse: (reply) => {
      // A shared client with ioredis's `stringNumbers` returns integer replies as decimal strings.
      const values = Array.isArray(reply) ? reply.map(Number) : []
      assert(values.length === 2 && values.every(Number.isSafeInteger), 'Publish script returned an unexpected reply')
      const [seq, timestamp] = values as [number, number]
      return { seq, timestamp }
    },
  }),
  headExchange: command({
    name: 'tfRoomHeadExchange',
    lua: HEAD_EXCHANGE_LUA,
    numberOfKeys: null,
    invoke: (prefix, { roomId, expected, next }: { roomId: string; expected: HeadExpectation; next: HeadNext }) => {
      const { counts = [] } = next
      const inc = next.head.currentInc
      const cells =
        counts.length === 0 || inc === null
          ? []
          : [
              revKey(prefix, roomId, inc),
              incarnationKeysKey(prefix, roomId, inc),
              cellIndexKey(prefix, roomId, inc),
              cellValuesKey(prefix, roomId, inc),
              cellVersionsKey(prefix, roomId, inc),
            ]
      return {
        keys: [headKey(prefix, roomId), ...cells],
        argv: [
          JSON.stringify(expected),
          encodeNext(next),
          ...counts.flatMap(({ key, delta, create }) => [key, String(delta), create ? '1' : '']),
        ],
      }
    },
    parse: (reply): HeadExchangeResult => {
      const parsed = JSON.parse(reply as string) as
        | { tag: 'head'; head: ScriptHead }
        | { tag: 'conflict'; current: ScriptHead | null }
      if (parsed.tag === 'head') return { kind: 'committed', head: toPublicHead(parsed.head) }
      return { kind: 'conflict', current: parsed.current === null ? null : toPublicHead(parsed.current) }
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
      return tag!.toString() === 'stale-incarnation' ? { kind: 'stale-incarnation' } : parseCells(now!, found)
    },
  }),
  readExpiringCells: command({
    name: 'tfRoomReadExpiringCells',
    lua: READ_EXPIRING_CELLS_LUA,
    numberOfKeys: null,
    binaryReply: true,
    invoke: (prefix, { roomId, inc, keys }: RoomInc & { keys: readonly string[] }) => ({
      keys: [headKey(prefix, roomId), ...keys.map((key) => expiringCellKey(prefix, roomId, inc, key))],
      argv: [inc, ...keys],
    }),
    parse: (reply): ExpiringCellsRead => {
      const [tag, ...found] = reply as Buffer[]
      if (tag!.toString() === 'stale-incarnation') return { kind: 'stale-incarnation' }
      const cells = new Map<string, Uint8Array>()
      for (let i = 0; i < found.length; i += 2) cells.set(found[i]!.toString(), Uint8Array.from(found[i + 1]!))
      return { kind: 'read', cells }
    },
  }),
  readCellPage: command({
    name: 'tfRoomReadCellPage',
    lua: READ_CELL_PAGE_LUA,
    numberOfKeys: 4,
    binaryReply: true,
    invoke: (
      prefix,
      { roomId, inc, cellPrefix, after, pageSize }: RoomInc & { cellPrefix: string; after?: string; pageSize: number },
    ) => ({
      keys: [
        headKey(prefix, roomId),
        cellIndexKey(prefix, roomId, inc),
        cellValuesKey(prefix, roomId, inc),
        cellVersionsKey(prefix, roomId, inc),
      ],
      argv: [inc, after === undefined ? `[${cellPrefix}` : `(${after}`, cellPrefix, String(pageSize)],
    }),
    parse: (reply): CellsRead & { after?: string } => {
      const [tag, now, ...found] = reply as Buffer[]
      if (tag!.toString() === 'stale-incarnation') return { kind: 'stale-incarnation' }
      if (tag!.toString() === 'cells') return parseCells(now!, found)
      return { ...parseCells(now!, found.slice(1)), after: found[0]!.toString() }
    },
  }),
  laneStart: command({
    name: 'tfRoomLaneStart',
    lua: LANE_START_LUA,
    numberOfKeys: 2,
    invoke: (prefix, { roomId, inc, lane, laneKey }: RoomSubscriptionSource & { laneKey: string }) => {
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
      return reply === '' ? new Map() : new Map([['', parseOrdering(reply as string)]])
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
      for (let i = 0; i < fields.length; i += 2) starts.push([fields[i]!, parseOrdering(fields[i + 1]!)])
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
    invoke: (prefix, { roomId, inc, lanes, kit }: RoomInc & { lanes: readonly string[]; kit: RoomPlaneKit }) => ({
      keys: lanes.map((laneKey) => memberOrderKey(prefix, roomId, inc, binaryLaneOf(kit, laneKey).member)),
      argv: lanes,
    }),
    parse: (reply) => (reply as string[]).map((mark) => (mark === '' ? null : parseOrdering(mark))),
  }),
  broadcastStart: command({
    name: 'tfBroadcastStart',
    lua: BROADCAST_START_LUA,
    numberOfKeys: 1,
    invoke: (prefix, route: BroadcastRoute) => ({ keys: [broadcastSequenceKey(prefix, route.key)], argv: [] }),
    // The lane of a route counts its kind's publishes, in which a hole is a lost message.
    parse: (reply, { kind }): Map<string, OrderingInfo> => {
      const [, timestamp, text, binary] = (reply as string).split(':').map(Number) as [number, number, number, number]
      return new Map([['', { seq: kind === 'text' ? text : binary, timestamp }]])
    },
  }),
  dropIncarnation: command({
    name: 'tfRoomDropIncarnation',
    lua: DROP_INCARNATION_LUA,
    numberOfKeys: 2,
    invoke: (prefix, { roomId, inc }: RoomInc) => ({
      keys: [incarnationInvalidationChannel(prefix, roomId, inc), incarnationKeysKey(prefix, roomId, inc)],
      argv: [inc],
    }),
    parse: () => {},
  }),
  cellsExchange: command({
    name: 'tfRoomCellsExchange',
    lua: CELLS_EXCHANGE_LUA,
    numberOfKeys: 6,
    invoke: (prefix, { roomId, inc, exchange }: RoomInc & { exchange: CellsExchange }) => {
      const { counts = [] } = exchange
      return {
        keys: [
          headKey(prefix, roomId),
          revKey(prefix, roomId, inc),
          incarnationKeysKey(prefix, roomId, inc),
          cellIndexKey(prefix, roomId, inc),
          cellValuesKey(prefix, roomId, inc),
          cellVersionsKey(prefix, roomId, inc),
        ],
        argv: [
          inc,
          String(exchange.mutations.length),
          String(exchange.expect.length),
          String(counts.length),
          ...exchange.mutations.flatMap(({ key, bytes }) =>
            bytes === null ? [key, 'del', ''] : [key, 'set', toBuffer(bytes)],
          ),
          ...exchange.expect.flatMap(({ key, version }) => [key, version ?? '']),
          ...counts.flatMap(({ key, delta, create }) => [key, String(delta), create ? '1' : '']),
        ],
      }
    },
    parse: (reply): CellsExchangeResult => {
      const [tag, ...counted] = reply as string[]
      if (tag !== 'committed') return { kind: tag as 'conflict' | 'stale-incarnation' }
      const counters = new Map<string, CellCounter>()
      for (let i = 0; i < counted.length; i += 3) {
        counters.set(counted[i]!, { value: Number(counted[i + 1]), changedAt: Number(counted[i + 2]) })
      }
      return { kind: 'committed', counters }
    },
  }),
  commit: command({
    name: 'tfRoomCommit',
    lua: COMMIT_LUA,
    numberOfKeys: Object.keys(COMMIT_KEYS).length,
    invoke: commitInvocation,
    parse: (reply, { requiredCellKeys }): CommitOutcome => {
      const parsed = JSON.parse(reply as string) as CommitReply
      if (parsed.kind !== 'stale-cell') return parsed
      const key = requiredCellKeys[parsed.index]
      assert(key !== undefined)
      return { kind: 'stale-cell', key }
    },
  }),
  operation: command({
    name: 'tfRoomOperation',
    lua: OPERATION_LUA,
    numberOfKeys: null,
    invoke: (prefix, { roomId, inc, writes, reads, members, commit, kit }: OperationInput) => {
      const semanticLaneKey = kit.encodeLaneKey({ kind: 'semantic' })
      const keys: string[] = []
      keys[OPERATION_KEYS.head] = headKey(prefix, roomId)
      keys[OPERATION_KEYS.rev] = revKey(prefix, roomId, inc)
      keys[OPERATION_KEYS.incarnationKeys] = incarnationKeysKey(prefix, roomId, inc)
      keys[OPERATION_KEYS.cellIndex] = cellIndexKey(prefix, roomId, inc)
      keys[OPERATION_KEYS.cellValues] = cellValuesKey(prefix, roomId, inc)
      keys[OPERATION_KEYS.cellVersions] = cellVersionsKey(prefix, roomId, inc)
      keys[OPERATION_KEYS.retainedLanes] = retainedLanesKey(prefix, roomId, inc)
      keys[OPERATION_KEYS.semanticRetained] = retainedKey(prefix, roomId, inc, semanticLaneKey)
      keys[OPERATION_KEYS.retainedOwners] = retainedOwnersKey(prefix, roomId, inc)
      const keysAt = (...added: string[]) => keys.push(...added) - added.length + 1
      const argv: (string | Buffer)[] = [inc, '']
      const mutations = writes.mutations.map(({ key, bytes }): [string, number] => {
        if (bytes === null) return [key, 0]
        argv.push(toBuffer(bytes))
        return [key, argv.length]
      })
      const retained = members
        .flatMap(({ retained }) => retained)
        .map((laneKey): [string, number] => {
          const { member, track } = binaryLaneOf(kit, laneKey)
          const at = keysAt(
            retainedKey(prefix, roomId, inc, laneKey),
            retainedOfKey(prefix, roomId, inc, member),
            retainedOfTrackKey(prefix, roomId, inc, track),
          )
          return [laneKey, at]
        })
      const trackLanes = members
        .flatMap(({ lanes }) => lanes.filter((key) => kit.decodeLaneKey(key).kind === 'binary'))
        .map((laneKey): [string, number] => [
          laneKey,
          keysAt(trackLanesKey(prefix, roomId, inc, binaryLaneOf(kit, laneKey).track)),
        ])
      const expiring = writes.expiring.map(({ key, bytes, ttlMs }): [number, number, number] => {
        argv.push(toBuffer(bytes))
        return [argv.length, ttlMs, keysAt(expiringCellKey(prefix, roomId, inc, key))]
      })
      let commitAt: OperationPacket['commit'] = []
      if (commit !== null) {
        const invocation = commitInvocation(prefix, commit)
        commitAt = [keysAt(...invocation.keys), argv.length + 1]
        argv.push(...invocation.argv)
      }
      const packet: OperationPacket = {
        expect: [...reads],
        members: members.map(({ member, lanes, retained }) => [
          member,
          lanes.length,
          retained.length,
          keysAt(memberOrderKey(prefix, roomId, inc, member), retainedOfKey(prefix, roomId, inc, member)),
        ]),
        semantic: semanticLaneKey,
        prefixes: writes.prefixes,
        mutations,
        counts: writes.counts.map(({ key, delta, create }) => [key, delta, create === true]),
        retained,
        trackLanes,
        expiring,
        commit: commitAt,
      }
      argv[1] = JSON.stringify(packet)
      return { keys, argv }
    },
    parse: (reply): OperationOutcome => {
      if (typeof reply === 'string') return JSON.parse(reply) as StaleIncarnation | { kind: 'conflict' }
      const [commit, ...counted] = reply as string[]
      const counters = new Map<string, CellCounter>()
      for (let i = 0; i < counted.length; i += 3) {
        counters.set(counted[i]!, { value: Number(counted[i + 1]), changedAt: Number(counted[i + 2]) })
      }
      return { kind: 'done', counters, commit: commit === 'null' ? null : (JSON.parse(commit!) as CommitReply) }
    },
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
function parseCells(now: Buffer, found: Buffer[]): Extract<CellsRead, { kind: 'read' }> {
  const cells = new Map<string, Uint8Array>()
  const versions = new Map<string, string>()
  for (let i = 0; i < found.length; i += 3) {
    const key = found[i]!.toString()
    cells.set(key, Uint8Array.from(found[i + 1]!))
    versions.set(key, found[i + 2]!.toString())
  }
  return { kind: 'read', cells, versions, now: Number(now.toString()) }
}

/** A copy, as ioredis may hold a command past the call (connecting, auto-pipelining, a NOSCRIPT or TRYAGAIN retry). */
function toBuffer(bytes: Uint8Array): Buffer {
  return Buffer.from(bytes)
}

function parseOrdering(reply: string): OrderingInfo {
  const [seq, timestamp] = reply.split(':').map(Number) as [number, number]
  return { seq, timestamp }
}

function binaryLaneOf(kit: RoomPlaneKit, laneKey: string): Extract<LaneId, { kind: 'binary' }> {
  const lane = kit.decodeLaneKey(laneKey)
  assert(lane.kind === 'binary')
  return lane
}

/** A larger message never arrives, and closes the connection all of a server's subscriptions share. */
function assertPublishable(bytes: number): void {
  if (bytes <= PUBLISH_MAX_BYTES) return
  throw new ChannelOverflowError(
    `Redis can't deliver a message of ${bytes} bytes: publish ${PUBLISH_MAX_BYTES} at most`,
  )
}

/** COMMIT's KEYS and ARGV, which OPERATION's commit takes too. */
function commitInvocation(prefix: string, input: CommitInput): { keys: string[]; argv: (string | Buffer)[] } {
  const { roomId, inc, lane, laneKey, channels, fenceTokens } = input
  assertPublishable(input.payload.byteLength)
  const retained = retainedKey(prefix, roomId, inc, laneKey)
  const indexed = lane.kind === 'binary' && input.retain
  const order =
    lane.kind === 'inbox' || lane.kind === 'binary'
      ? memberOrderKey(prefix, roomId, inc, lane.member)
      : orderKey(prefix, roomId, inc, laneKey)
  const keys: string[] = []
  keys[COMMIT_KEYS.head] = headKey(prefix, roomId)
  keys[COMMIT_KEYS.order] = order
  keys[COMMIT_KEYS.retained] = retained
  keys[COMMIT_KEYS.channel] = channels.lane
  keys[COMMIT_KEYS.incarnationKeys] = incarnationKeysKey(prefix, roomId, inc)
  keys[COMMIT_KEYS.cellValues] = cellValuesKey(prefix, roomId, inc)
  keys[COMMIT_KEYS.everyTrackChannel] = channels.everyTrack ?? channels.lane
  keys[COMMIT_KEYS.memberRetained] = indexed ? retainedOfKey(prefix, roomId, inc, lane.member) : retained
  keys[COMMIT_KEYS.trackRetained] = indexed ? retainedOfTrackKey(prefix, roomId, inc, lane.track) : retained
  keys[COMMIT_KEYS.everyMemberChannel] = channels.everyMember ?? channels.lane
  keys[COMMIT_KEYS.trackLanes] = lane.kind === 'binary' ? trackLanesKey(prefix, roomId, inc, lane.track) : order
  keys[COMMIT_KEYS.retainedLanes] = retainedLanesKey(prefix, roomId, inc)
  keys[COMMIT_KEYS.retainedOwners] = retainedOwnersKey(prefix, roomId, inc)
  const argv: (string | Buffer)[] = []
  argv[COMMIT_ARGS.inc] = inc
  argv[COMMIT_ARGS.laneKind] = lane.kind
  argv[COMMIT_ARGS.closingLease] = input.closingLease ?? ''
  argv[COMMIT_ARGS.retain] = input.retain ? '1' : '0'
  argv[COMMIT_ARGS.payload] = toBuffer(input.payload)
  argv[COMMIT_ARGS.channelFence] = fenceTokens.lane
  argv[COMMIT_ARGS.everyTrackFence] = fenceTokens.everyTrack
  argv[COMMIT_ARGS.laneKey] = laneKey
  argv[COMMIT_ARGS.everyMemberFence] = fenceTokens.everyMember
  argv[COMMIT_ARGS.owner] = input.owner ?? ''
  for (const key of input.requiredCellKeys) argv.push(key)
  return { keys, argv }
}
