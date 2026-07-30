// Final physical key layout + every Lua script for the Redis BackendDriver. The module is
// package-internal but is emitted because RedisRoomBackend consumes it; standalone and Cluster use this one layout.
//
// Layout — every key of one room shares the `{<rid>}` hash tag, so a whole room lives in ONE Cluster
// slot and each script can declare all the keys it touches in KEYS (spi.md §5.2):
//
//   head:      tf:room:{rid}:head          JSON { rev, state, config(b64), inc?, lease?{id,until}, exp? }
//   headrev:   tf:room:{rid}:headrev        INCR counter — the monotonic source of every head `rev`
//   cells:     tf:room:{rid}:g:<inc>:c:<key>   logical cell:  "<expiresAt|''>\n<bytes>"  (PX = backstop)
//   revision:  tf:room:{rid}:g:<inc>:rev    INCR'd by every cell CX — the coarse per-generation revision
//   order:     tf:room:{rid}:g:<inc>:o:<laneKey>   "<seq>:<ts>"
//   retained:  tf:room:{rid}:g:<inc>:rt:<laneKey>  16-byte [seq_hi][seq_lo][ts_hi][ts_lo] + payload
//   rt-size:   tf:room:{rid}:g:<inc>:rt-size       aggregate retained PAYLOAD bytes (headers excluded)
//   channels:  tf:room:{rid}:ch:<inc>:<laneKey>    PUBLISH/SUBSCRIBE — INC-SCOPED (an old-inc SUBSCRIBE
//                                                  can never hear a recreation — I11)
//   gens:      tf:room:{rid}:gens           SET of incs — SADD'd by the head-CX that installs an inc,
//                                           SREM'd by dropGeneration; the fresh-inc guard is one SISMEMBER
//   gen-token: tf:room:{rid}:gen-tokens      HASH inc -> non-reusable generation token (the installing
//                                           head revision), removed only with the final gens SREM
//   dir index: tf:{rid-dir}<prefix>… — the directory is global, its own two co-slotted keys (backend.ts)
//
// AUTHORITY TIME: production derives `now_ms` from `redis.call('TIME')` (the one central clock, atomic
// inside the script — spi.md I13). Every time-sensitive command accepts an optional `now_ms` scalar so
// the atomic Lua protocol can be tested against a controlled authority clock; RedisRoomBackend supplies
// an empty value in production and therefore uses Redis TIME. The scalar is never a key and cannot affect
// the co-slot invariant. A caller-local `Date.now()` is never an authority source.

import { HEAD_TRANSITIONS, ORDERING_FRAME_LAYOUT, laneKey, type LaneId } from 'telefunc/backend'
export { laneKey }

export const DEFAULT_ROOM_PREFIX = 'tf:'
export const REDIS_DELIVERY_FENCE_BYTE = 0xff
export const REDIS_SAFE_INTEGER_MAX = Number.MAX_SAFE_INTEGER

// ── key naming ────────────────────────────────────────────────────────────

// `{<rid>}` is the Cluster hash tag; every per-room key carries it so the room is one slot.
export function roomTag(prefix: string, roomId: string): string {
  // A Redis hash tag ends at the first `}`. Encode caller input before placing it in braces so an
  // arbitrary room id cannot escape the tag or split one logical room across slots.
  return `${prefix}room:{${encodeURIComponent(roomId)}}`
}
export function headKey(prefix: string, roomId: string): string {
  return `${roomTag(prefix, roomId)}:head`
}
export function headRevKey(prefix: string, roomId: string): string {
  return `${roomTag(prefix, roomId)}:headrev`
}
export function gensKey(prefix: string, roomId: string): string {
  return `${roomTag(prefix, roomId)}:gens`
}
export function generationTokensKey(prefix: string, roomId: string): string {
  return `${roomTag(prefix, roomId)}:gen-tokens`
}
export function genPrefix(prefix: string, roomId: string, inc: string): string {
  return `${roomTag(prefix, roomId)}:g:${inc}`
}
export function revKey(prefix: string, roomId: string, inc: string): string {
  return `${genPrefix(prefix, roomId, inc)}:rev`
}
export function cellKeyPrefix(prefix: string, roomId: string, inc: string): string {
  return `${genPrefix(prefix, roomId, inc)}:c:`
}
export function cellKey(prefix: string, roomId: string, inc: string, key: string): string {
  return `${cellKeyPrefix(prefix, roomId, inc)}${key}`
}
export function orderKey(prefix: string, roomId: string, inc: string, laneKey: string): string {
  return `${genPrefix(prefix, roomId, inc)}:o:${laneKey}`
}
export function retainedKeyPrefix(prefix: string, roomId: string, inc: string): string {
  return `${genPrefix(prefix, roomId, inc)}:rt:`
}
export function retainedKey(prefix: string, roomId: string, inc: string, laneKey: string): string {
  return `${retainedKeyPrefix(prefix, roomId, inc)}${laneKey}`
}
export function retainedSizeKey(prefix: string, roomId: string, inc: string): string {
  return `${genPrefix(prefix, roomId, inc)}:rt-size`
}
export function channelKey(prefix: string, roomId: string, inc: string, laneKey: string): string {
  return `${roomTag(prefix, roomId)}:ch:${inc}:${laneKey}`
}
export function generationInvalidationChannel(prefix: string, roomId: string, inc: string): string {
  return `${roomTag(prefix, roomId)}:invalidate:${inc}`
}
// The directory's two keys share their own tag so the tag-guarded delete stays one slot under Cluster.
export function directoryIndexKey(prefix: string): string {
  return `${prefix}room-dir:{${prefix}dir}:index`
}
export function directoryTagsKey(prefix: string): string {
  return `${prefix}room-dir:{${prefix}dir}:tags`
}

export function parseLaneKey(key: string): LaneId {
  if (key === 'semantic') return { kind: 'semantic' }
  if (key === 'control') return { kind: 'control' }
  if (key.startsWith('binary:')) {
    const [, member, track] = key.split(':')
    return { kind: 'binary', member: decodeURIComponent(member ?? ''), track: decodeURIComponent(track ?? '') }
  }
  if (key.startsWith('inbox:')) {
    const [, member] = key.split(':')
    return { kind: 'inbox', member: decodeURIComponent(member ?? '') }
  }
  throw new Error(`parseLaneKey: unrecognized lane key '${key}'`)
}

// SCAN's MATCH is a glob; a physical key built from a caller-supplied room id / cell key must match
// literally, so escape the glob metacharacters.
export function escapeGlob(pattern: string): string {
  return pattern.replace(/[*?[\]\\^]/g, '\\$&')
}

// ── Lua ─────────────────────────────────────────────────────────────────

// Shared preamble: authority time, either the injected frozen clock (conformance) or the central
// server clock (production). ms precision from Redis TIME's [sec, µs] pair.
const NOW_FN = `
local function tf_now(v)
  if v ~= nil and v ~= '' then return tonumber(v) end
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
-- read the head, treating a logically-expired tombstone as absent (a lapsed tombstone reopens the
-- absence epoch — I1); the PX backstop only reclaims memory, it is never what makes it invisible.
local function tf_head(key, now)
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

// Redis renders the frozen, backend-neutral ordering layout into its own runtime language. Both generic
// Broadcast and durable Room embed this exact function; the decoder below interprets the same data.
const orderingFrameFormat = [
  ORDERING_FRAME_LAYOUT.endianness === 'big' ? '>' : '<',
  ...Object.values(ORDERING_FRAME_LAYOUT.offsets)
    .sort((left, right) => left - right)
    .map(() => `I${ORDERING_FRAME_LAYOUT.wordBytes}`),
].join('')
export const REDIS_ORDERING_FRAME_LUA = `
local function tf_ordering_frame(seq, ts, payload)
  local seq_hi = math.floor(seq / ${ORDERING_FRAME_LAYOUT.wordRange})
  local seq_lo = seq - seq_hi * ${ORDERING_FRAME_LAYOUT.wordRange}
  local ts_hi = math.floor(ts / ${ORDERING_FRAME_LAYOUT.wordRange})
  local ts_lo = ts - ts_hi * ${ORDERING_FRAME_LAYOUT.wordRange}
  return struct.pack('${orderingFrameFormat}', seq_hi, seq_lo, ts_hi, ts_lo) .. payload
end
`

export function decodeRedisOrderingFrame(frame: Uint8Array): {
  payload: Uint8Array
  info: { seq: number; timestamp: number }
} {
  if (frame.byteLength < ORDERING_FRAME_LAYOUT.headerBytes) {
    throw new Error(`Redis ordering frame is shorter than its ${ORDERING_FRAME_LAYOUT.headerBytes}-byte header`)
  }
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength)
  const littleEndian = String(ORDERING_FRAME_LAYOUT.endianness) === 'little'
  const { offsets, wordRange } = ORDERING_FRAME_LAYOUT
  const info = {
    seq: view.getUint32(offsets.seqHigh, littleEndian) * wordRange + view.getUint32(offsets.seqLow, littleEndian),
    timestamp:
      view.getUint32(offsets.timestampHigh, littleEndian) * wordRange +
      view.getUint32(offsets.timestampLow, littleEndian),
  }
  if (!Number.isSafeInteger(info.seq) || info.seq <= 0) {
    throw new Error('Redis ordering frame has an invalid sequence')
  }
  if (!Number.isSafeInteger(info.timestamp) || info.timestamp < 0) {
    throw new Error('Redis ordering frame has an invalid timestamp')
  }
  return { payload: frame.subarray(ORDERING_FRAME_LAYOUT.headerBytes), info }
}

function renderLuaHeadTransitionTable(variable = 'HEAD_TRANSITIONS'): string {
  const rows = HEAD_TRANSITIONS.map((rule) => {
    const key = `${rule.from}|${rule.cx}|${rule.to}`
    return `  ["${key}"] = "${rule.constraint}",`
  })
  return [`local ${variable} = {`, ...rows, '}'].join('\n')
}

// HEAD CX — the single lifecycle primitive. One atomic record does legality (throw), compare (conflict),
// the fresh-inc guard, minting and the store. The guarded transitions cannot be reached through any
// other compare form (spi.md §2 transition table), so a generic {rev} can never install/replace a lease,
// re-lease a live 'closing' head, or reach 'closed'.
//   KEYS: [1]=head [2]=gens [3]=headrev [4]=generation-tokens
//   ARGV: [1]=now [2]=cxJson{form,rev?,closingLease?} [3]=nextJson{kind,state?,inc?,config?,lease?,ttlMs?}
export const HEAD_CX_LUA = `${NOW_FN}
${renderLuaHeadTransitionTable()}
local head_key, gens_key, rev_key, generation_tokens_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local now = tf_now(ARGV[1])
local cx = cjson.decode(ARGV[2])
local nx = cjson.decode(ARGV[3])
local cur = tf_head(head_key, now)
local from = 'absent'
if cur then from = cur.state end

local function conflict()
  if cur then return '{"tag":"conflict","current":' .. cjson.encode(cur) .. '}' end
  return '{"tag":"conflict","current":null}'
end

-- Operation legality of the tombstone delete is decided BEFORE any compare, so misuse throws even where
-- the compare would have conflicted (spi.md §2 — the delete row only). A legal delete still goes through
-- the selected HeadCx compare form below: guarded takeover/finalize forms cannot bypass their predicates.
if nx.kind == 'delete' then
  if from ~= 'closed' then
    return redis.error_reply("head CX: {delete} is legal only against a 'closed' tombstone, not '" .. from .. "'")
  end
end

-- compare, by cx form
local matches = false
if cx.form == 'absent' then
  matches = (cur == nil)
elseif cur ~= nil and cur.rev == cx.rev then
  if cx.form == 'takeover' then
    matches = (cur.state == 'closing' and cur.lease ~= nil and cur.lease['until'] < now)
  elseif cx.form == 'finalize' then
    matches = (cur.state == 'closing' and cur.lease ~= nil and cur.lease.id == cx.closingLease)
  else
    matches = true
  end
end
if not matches then return conflict() end

if nx.kind == 'delete' then
  redis.call('DEL', head_key)
  return '{"tag":"deleted"}'
end

-- Interpret the generated transition table against the head the compare matched.
local transition = from .. ' + ' .. cx.form .. ' -> ' .. nx.state
local constraint = HEAD_TRANSITIONS[from .. '|' .. cx.form .. '|' .. nx.state]
if not constraint then
  return redis.error_reply("head CX: '" .. transition .. "' is not a legal head transition")
end
local installs_generation = constraint == 'fresh-inc'
if constraint == 'fresh-inc' then
  if nx.inc ~= nil and redis.call('SISMEMBER', gens_key, nx.inc) == 1 then
    return redis.error_reply("head CX: incarnation '" .. tostring(nx.inc) ..
      "' still has surviving generation state — creating a room on it would resurrect it")
  end
elseif constraint == 'same-inc' then
  if nx.inc ~= cur.inc then
    return redis.error_reply('head CX: ' .. transition .. ' must keep the same incarnation')
  end
elseif constraint == 'replace-lease' then
  if nx.inc ~= cur.inc then
    return redis.error_reply('head CX: an expired-close takeover must keep the same incarnation')
  end
  if nx.config ~= cur.config then
    return redis.error_reply(
      'head CX: an expired-close takeover must not change the config — it replaces only the lease')
  end
  if nx.lease.id == cur.lease.id then
    return redis.error_reply('head CX: an expired-close takeover must mint a different lease id')
  end
end

-- apply: mint the lease deadline from authority time inside this same atomic record, store, register gen
local stored = { rev = 'rev-' .. redis.call('INCR', rev_key), state = nx.state, config = nx.config }
if nx.inc ~= nil then stored.inc = nx.inc end
if nx.lease ~= nil then stored.lease = { id = nx.lease.id, ['until'] = now + nx.lease.durationMs } end
if nx.ttlMs ~= nil then stored.exp = now + nx.ttlMs end
local encoded = cjson.encode(stored)
redis.call('SET', head_key, encoded)
if nx.ttlMs ~= nil then redis.call('PEXPIRE', head_key, nx.ttlMs) end
if nx.inc ~= nil then
  redis.call('SADD', gens_key, nx.inc)
  if installs_generation then redis.call('HSET', generation_tokens_key, nx.inc, stored.rev) end
end
return '{"tag":"head","head":' .. encoded .. '}'
`

export const HEAD_CX_CMD = 'tfRoomHeadCx'
export const HEAD_CX_KEYS = 4

// SUBSCRIBE establishment snapshots the generation token before its network await. This atomic
// post-ack check rejects a token whose generation closed or was dropped during that await.
//   KEYS: [1]=head [2]=gens [3]=generation-tokens
//   ARGV: [1]=now [2]=inc [3]=expected-token
export const VALIDATE_GENERATION_LUA = `${NOW_FN}
local head_key, gens_key, tokens_key = KEYS[1], KEYS[2], KEYS[3]
local now = tf_now(ARGV[1])
local inc, expected_token = ARGV[2], ARGV[3]
local head = tf_head(head_key, now)
local current_token = redis.call('HGET', tokens_key, inc)
if not head or head.state ~= 'open' or head.inc ~= inc
  or redis.call('SISMEMBER', gens_key, inc) ~= 1 or current_token ~= expected_token then
  return 0
end
return 1
`

export const VALIDATE_GENERATION_CMD = 'tfRoomValidateGeneration'
export const VALIDATE_GENERATION_KEYS = 3

// The durable invalidation sources are removed atomically and LAST, after every fallible local
// UNSUBSCRIBE cleanup succeeds. A failed cleanup therefore leaves both values available for retry.
export const DROP_GENERATION_FINALIZE_LUA = `
redis.call('SREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
`

export const DROP_GENERATION_FINALIZE_CMD = 'tfRoomDropGenerationFinalize'
export const DROP_GENERATION_FINALIZE_KEYS = 2

// CELLS CX — all mutations or none; success implies the head precondition (open + inc) held at apply
// time; the revision is the coarse per-generation counter, allowed to over-conflict but never mislead.
//   KEYS: [1]=head [2]=rev [3..]=cell keys (one per mutation, in order)
//   ARGV: [1]=now [2]=inc [3]=expectedRev, then per mutation: op('set'|'del'), ttlMs(''|number), value
export const CELLS_CX_LUA = `${NOW_FN}
local head_key, rev_key = KEYS[1], KEYS[2]
local now = tf_now(ARGV[1])
local head = tf_head(head_key, now)
if (not head) or head.inc ~= ARGV[2] or head.state ~= 'open' then return 'stale-inc' end
local cur = redis.call('GET', rev_key)
if not cur then cur = '0' end
if cur ~= ARGV[3] then return 'conflict' end
local n = #KEYS - 2
for i = 1, n do
  local key = KEYS[2 + i]
  local base = 3 + (i - 1) * 3
  local op = ARGV[base + 1]
  if op == 'del' then
    redis.call('DEL', key)
  else
    local ttl = ARGV[base + 2]
    local val = ARGV[base + 3]
    local head_str = ''
    if ttl ~= '' then head_str = tostring(now + tonumber(ttl)) end
    local stored = head_str .. '\\n' .. val
    if ttl == '' then
      redis.call('SET', key, stored)
    else
      redis.call('SET', key, stored, 'PX', tonumber(ttl))
    end
  end
end
redis.call('INCR', rev_key)
return 'committed'
`

export const CELLS_CX_CMD = 'tfRoomCellsCx'

// COMMIT — atomic acceptance: head precondition (one boolean, two branches), retained aggregate-cap
// validation, order advance, optional retained install, then PUBLISH. Supplying a closing lease selects
// the narrow closing-control branch, which is what makes every other lane stale while closing (I12).
//   KEYS: [1]=head [2]=order [3]=retained [4]=channel [5]=retained aggregate payload size
//         [6..]=required live cells
//   ARGV: [1]=now [2]=inc [3]=laneKind [4]=closingLease('') [5]=retain('0'|'1')
//         [6]=payload [7]=aggregate retained payload cap [8]=local delivery-fence token or ''
export const COMMIT_LUA = `${NOW_FN}
${REDIS_ORDERING_FRAME_LUA}
local head_key, order_key, retained_key, channel_key, retained_size_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local now = tf_now(ARGV[1])
local head = tf_head(head_key, now)
local ok = false
if head and head.inc == ARGV[2] then
  if ARGV[4] == '' then
    ok = (head.state == 'open')
  else
    ok = (ARGV[3] == 'control' and head.state == 'closing' and head.lease ~= nil
          and head.lease.id == ARGV[4] and now <= head.lease['until'])
  end
end
if not ok then return '{"stale":true}' end
for i = 6, #KEYS do
  local raw = redis.call('GET', KEYS[i])
  if not raw then return '{"stale":true}' end
  local newline = string.find(raw, '\\n', 1, true)
  if newline then
    local expires_at = string.sub(raw, 1, newline - 1)
    if expires_at ~= '' and tonumber(expires_at) <= now then return '{"stale":true}' end
  end
end

-- The counter is aggregate PAYLOAD bytes. A stored frame has a fixed 16-byte order header, excluded
-- when replacing an existing retained lane. The check precedes every acceptance mutation.
local retained_total = nil
if ARGV[5] == '1' then
  local current_total = tonumber(redis.call('GET', retained_size_key) or '0')
  local old_frame_bytes = redis.call('STRLEN', retained_key)
  local old_payload_bytes = 0
  if old_frame_bytes > ${ORDERING_FRAME_LAYOUT.headerBytes} then
    old_payload_bytes = old_frame_bytes - ${ORDERING_FRAME_LAYOUT.headerBytes}
  end
  retained_total = current_total - old_payload_bytes + string.len(ARGV[6])
  if retained_total > tonumber(ARGV[7]) then
    return redis.error_reply('commitLane: retained aggregate ' .. retained_total .. ' bytes exceeds the ' .. ARGV[7] .. ' byte cap')
  end
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
  if not base_seq or not base_ts or base_seq < 0 or base_seq > ${REDIS_SAFE_INTEGER_MAX}
      or base_ts < 0 or base_ts > ${REDIS_SAFE_INTEGER_MAX} then
    return redis.error_reply('commitLane: invalid ordering watermark')
  end
end
if base_seq >= ${REDIS_SAFE_INTEGER_MAX} then
  return redis.error_reply('commitLane: sequence exhausted for the ordering domain')
end
local seq = base_seq + 1
local ts = now
if base_ts > ts then ts = base_ts end
if seq < 1 or seq > ${REDIS_SAFE_INTEGER_MAX} or ts < 0 or ts > ${REDIS_SAFE_INTEGER_MAX} then
  return redis.error_reply('commitLane: invalid ordering position')
end
-- Lua's implicit number-to-string conversion uses limited significant digits at the safe-integer
-- boundary. Format both exact integers once, then use those decimal strings for durable state and the
-- JSON receipt so the final legal commit cannot effect successfully and fail only while decoding reply.
local seq_text = string.format('%.0f', seq)
local ts_text = string.format('%.0f', ts)
redis.call('SET', order_key, seq_text .. ':' .. ts_text)
local frame = tf_ordering_frame(seq, ts, ARGV[6])
if ARGV[5] == '1' then
  redis.call('SET', retained_key, frame)
  redis.call('SET', retained_size_key, retained_total)
end
local receivers = redis.call('PUBLISH', channel_key, frame)
-- A Cluster forwards both publications from this slot owner over the same ordered bus link. The
-- impossible ordering-frame prefix makes the second publication an internal local-dispatch fence.
if ARGV[8] ~= '' then redis.call('PUBLISH', channel_key, string.char(${REDIS_DELIVERY_FENCE_BYTE}) .. ARGV[8]) end
return '{"accepted":true,"seq":' .. seq_text .. ',"timestamp":' .. ts_text .. ',"receivers":' .. receivers .. '}'
`

export const COMMIT_CMD = 'tfRoomCommit'

// Retained deletion updates the aggregate payload counter in the same atomic record as payload removal.
// KEYS[1] is the counter; KEYS[2..] are the selected retained lane keys.
export const RETAINED_DELETE_LUA = `
local size_key = KEYS[1]
local total = tonumber(redis.call('GET', size_key) or '0')
local if_seq = ARGV[1]
if if_seq ~= '' then
  if #KEYS ~= 2 then return redis.error_reply('deleteRetained: ifSeq requires one lane') end
  local expected = tonumber(if_seq)
  if not expected or expected < 1 or expected > ${REDIS_SAFE_INTEGER_MAX} or expected ~= math.floor(expected) then
    return redis.error_reply('deleteRetained: invalid ifSeq')
  end
  local frame = redis.call('GET', KEYS[2])
  if not frame then return total end
  if string.len(frame) < ${ORDERING_FRAME_LAYOUT.headerBytes} then
    return redis.error_reply('deleteRetained: invalid retained frame')
  end
  local seq_hi, seq_lo = struct.unpack('>I4I4', frame)
  local current = seq_hi * 4294967296 + seq_lo
  if current ~= expected then return total end
end
for i = 2, #KEYS do
  local frame_bytes = redis.call('STRLEN', KEYS[i])
  if frame_bytes > 0 then
    local payload_bytes = 0
    if frame_bytes > ${ORDERING_FRAME_LAYOUT.headerBytes} then
      payload_bytes = frame_bytes - ${ORDERING_FRAME_LAYOUT.headerBytes}
    end
    total = total - payload_bytes
    redis.call('DEL', KEYS[i])
  end
end
if total <= 0 then
  redis.call('DEL', size_key)
  total = 0
else
  redis.call('SET', size_key, total)
end
return total
`

export const RETAINED_DELETE_CMD = 'tfRoomRetainedDelete'

// Directory records use two co-slotted global keys. Put and compare-delete are each one atomic record,
// so stale cleanup cannot erase (or de-index) a concurrent newer tag.
export const DIRECTORY_PUT_LUA = `
redis.call('ZADD', KEYS[1], 0, ARGV[1])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
return 1
`

export const DIRECTORY_PUT_CMD = 'tfRoomDirectoryPut'
export const DIRECTORY_PUT_KEYS = 2

export const DIRECTORY_DELETE_LUA = `
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
`

export const DIRECTORY_DELETE_CMD = 'tfRoomDirectoryDelete'
export const DIRECTORY_DELETE_KEYS = 2

// One production-owned inventory drives command registration and key assembly. Tests consume the same
// descriptors and builders, so a new script or a changed operand cannot silently escape slot/Lua proof.
export const REDIS_ROOM_COMMANDS = {
  headCx: { name: HEAD_CX_CMD, lua: HEAD_CX_LUA, numberOfKeys: HEAD_CX_KEYS },
  validateGeneration: {
    name: VALIDATE_GENERATION_CMD,
    lua: VALIDATE_GENERATION_LUA,
    numberOfKeys: VALIDATE_GENERATION_KEYS,
  },
  dropGenerationFinalize: {
    name: DROP_GENERATION_FINALIZE_CMD,
    lua: DROP_GENERATION_FINALIZE_LUA,
    numberOfKeys: DROP_GENERATION_FINALIZE_KEYS,
  },
  cellsCx: { name: CELLS_CX_CMD, lua: CELLS_CX_LUA, numberOfKeys: null },
  commit: { name: COMMIT_CMD, lua: COMMIT_LUA, numberOfKeys: null },
  retainedDelete: { name: RETAINED_DELETE_CMD, lua: RETAINED_DELETE_LUA, numberOfKeys: null },
  directoryPut: { name: DIRECTORY_PUT_CMD, lua: DIRECTORY_PUT_LUA, numberOfKeys: DIRECTORY_PUT_KEYS },
  directoryDelete: {
    name: DIRECTORY_DELETE_CMD,
    lua: DIRECTORY_DELETE_LUA,
    numberOfKeys: DIRECTORY_DELETE_KEYS,
  },
} as const

export const REDIS_ROOM_COMMAND_KEYS = {
  headCx: (prefix: string, roomId: string) => [
    headKey(prefix, roomId),
    gensKey(prefix, roomId),
    headRevKey(prefix, roomId),
    generationTokensKey(prefix, roomId),
  ],
  validateGeneration: (prefix: string, roomId: string) => [
    headKey(prefix, roomId),
    gensKey(prefix, roomId),
    generationTokensKey(prefix, roomId),
  ],
  dropGenerationFinalize: (prefix: string, roomId: string) => [
    gensKey(prefix, roomId),
    generationTokensKey(prefix, roomId),
  ],
  cellsCx: (prefix: string, roomId: string, inc: string, cells: readonly string[]) => [
    headKey(prefix, roomId),
    revKey(prefix, roomId, inc),
    ...cells.map((key) => cellKey(prefix, roomId, inc, key)),
  ],
  commit: (prefix: string, roomId: string, inc: string, lane: LaneId, requiredCellKeys: readonly string[] = []) => {
    const key = laneKey(lane)
    return [
      headKey(prefix, roomId),
      orderKey(prefix, roomId, inc, key),
      retainedKey(prefix, roomId, inc, key),
      channelKey(prefix, roomId, inc, key),
      retainedSizeKey(prefix, roomId, inc),
      ...requiredCellKeys.map((required) => cellKey(prefix, roomId, inc, required)),
    ]
  },
  retainedDelete: (prefix: string, roomId: string, inc: string, retainedKeys: readonly string[]) => [
    retainedSizeKey(prefix, roomId, inc),
    ...retainedKeys,
  ],
  directoryPut: (prefix: string) => [directoryIndexKey(prefix), directoryTagsKey(prefix)],
  directoryDelete: (prefix: string) => [directoryIndexKey(prefix), directoryTagsKey(prefix)],
} as const
