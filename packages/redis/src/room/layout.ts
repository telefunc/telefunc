// Final physical key layout + every Lua script for the Redis realization of RoomBackendSpi.
//
// DARK: this file is not exported from `@telefunc/redis`'s barrel and no Room call site reaches it. It
// is the standalone-Redis spike proved against the shared conformance suite (convergence W2); the
// hash-tagged one-slot Cluster proof is W4-R, so `capabilities.clusterSafe` stays false in backend.ts.
//
// Layout — every key of one room shares the `{<rid>}` hash tag, so a whole room lives in ONE Cluster
// slot and each script can declare all the keys it touches in KEYS (spi.md §5.2):
//
//   head:      tf:room:{rid}:head          JSON { rev, state, config(b64), inc?, lease?{id,until}, exp? }
//   headrev:   tf:room:{rid}:headrev        INCR counter — the monotonic source of every head `rev`
//   cells:     tf:room:{rid}:g:<inc>:c:<key>   logical cell:  "<expiresAt|''>\n<bytes>"  (PX = backstop)
//   revision:  tf:room:{rid}:g:<inc>:rev    INCR'd by every cell CX — the coarse per-generation revision
//   order:     tf:room:{rid}:g:<inc>:o:<laneKey>   "<seq>:<ts>:<expiresAt|''>"
//   retained:  tf:room:{rid}:g:<inc>:rt:<laneKey>  12-byte framed [seq][ts_hi][ts_lo] .. payload bytes
//   rt-size:   tf:room:{rid}:g:<inc>:rt-size       aggregate retained PAYLOAD bytes (headers excluded)
//   channels:  tf:room:{rid}:ch:<inc>:<laneKey>    PUBLISH/SUBSCRIBE — INC-SCOPED (an old-inc SUBSCRIBE
//                                                  can never hear a recreation — I11)
//   gens:      tf:room:{rid}:gens           SET of incs — SADD'd by the head-CX that installs an inc,
//                                           SREM'd by dropGeneration; the fresh-inc guard is one SISMEMBER
//   gen-token: tf:room:{rid}:gen-tokens      HASH inc -> non-reusable generation token (the installing
//                                           head revision), removed only with the final gens SREM
//   captures:  tf:room:{rid}:route-captures  HASH attempt id -> bounded generation-capture record;
//              tf:room:{rid}:route-capture-exp ZSET of authority expiry -> attempt id for lazy sweep
//   dir index: tf:{rid-dir}<prefix>… — the directory is global, its own two co-slotted keys (backend.ts)
//
// AUTHORITY TIME: production derives `now_ms` from `redis.call('TIME')` (the one central clock, atomic
// inside the script — spi.md I13). Every time-sensitive script also accepts an OPTIONAL injected
// `now_ms` ARGV: the conformance fixture drives a frozen, advanceable authority clock through it (a real
// Redis server clock cannot be advanced), and a backend that instead read a caller's local clock still
// fails every I13 killer because the injected value is the *shared* authority clock, never `Date.now()`.
// The seam is a scalar ARGV, never a key, so it does not touch the co-slot invariant.

import type { LaneId } from '../../../telefunc/wire-protocol/backend/spi.js'

export const DEFAULT_ROOM_PREFIX = 'tf:'

// ── key naming ────────────────────────────────────────────────────────────

// `{<rid>}` is the Cluster hash tag; every per-room key carries it so the room is one slot.
export function roomTag(prefix: string, roomId: string): string {
  return `${prefix}room:{${roomId}}`
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
export function routeCapturesKey(prefix: string, roomId: string): string {
  return `${roomTag(prefix, roomId)}:route-captures`
}
export function routeCaptureExpiriesKey(prefix: string, roomId: string): string {
  return `${roomTag(prefix, roomId)}:route-capture-exp`
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

// In the fixed lane table each lane's order domain and channel correspond one to one, so a single
// laneKey indexes both. member/track are percent-encoded so a member named `a:b` cannot collide.
export function laneKey(lane: LaneId): string {
  switch (lane.kind) {
    case 'semantic':
      return 'semantic'
    case 'control':
      return 'control'
    case 'binary':
      return `binary:${encodeURIComponent(lane.member)}:${encodeURIComponent(lane.track)}`
    case 'inbox':
      return `inbox:${encodeURIComponent(lane.member)}`
  }
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

// HEAD CX — the single lifecycle primitive. One atomic record does legality (throw), compare (conflict),
// the fresh-inc guard, minting and the store. The guarded transitions cannot be reached through any
// other compare form (spi.md §2 transition table), so a generic {rev} can never install/replace a lease,
// re-lease a live 'closing' head, or reach 'closed'.
//   KEYS: [1]=head [2]=gens [3]=headrev [4]=generation-tokens
//   ARGV: [1]=now [2]=cxJson{form,rev?,closingLease?} [3]=nextJson{kind,state?,inc?,config?,lease?,ttlMs?}
export const HEAD_CX_LUA = `${NOW_FN}
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

-- transition-table legality (throws), validated against the head the compare matched
local transition = from .. ' + ' .. cx.form .. ' -> ' .. nx.state
local installs_generation = transition == 'absent + absent -> open' or transition == 'closed + generic -> open'
if installs_generation then
  if nx.inc ~= nil and redis.call('SISMEMBER', gens_key, nx.inc) == 1 then
    return redis.error_reply("head CX: incarnation '" .. tostring(nx.inc) .. "' still has surviving generation state")
  end
elseif transition == 'open + generic -> open' or transition == 'open + generic -> closing' then
  if nx.inc ~= cur.inc then
    return redis.error_reply('head CX: ' .. transition .. ' must keep the same incarnation')
  end
elseif transition == 'closing + takeover -> closing' then
  if nx.inc ~= cur.inc then
    return redis.error_reply('head CX: an expired-close takeover must keep the same incarnation')
  end
  if nx.config ~= cur.config then
    return redis.error_reply('head CX: an expired-close takeover must not change the config')
  end
  if nx.lease.id == cur.lease.id then
    return redis.error_reply('head CX: an expired-close takeover must mint a different lease id')
  end
elseif transition == 'closing + finalize -> closed' then
  -- ok
else
  return redis.error_reply("head CX: '" .. transition .. "' is not a legal head transition")
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

// SUBSCRIBE establishment is a network operation outside Lua. Capture therefore pins the exact,
// authority-owned generation token before that await. The stable attempt id makes a lost first response
// idempotent; the authority-aligned creation epoch distinguishes a genuinely fresh absent attempt from
// a delayed retry after bounded reclamation. Expired rows are swept mechanically at the start of every
// capture, so one abandoned attempt cannot grow unbounded while the room remains active.
//   KEYS: [1]=head [2]=gens [3]=generation-tokens [4]=captures-hash [5]=capture-expiry-zset
//   ARGV: [1]=now [2]=inc [3]=attempt-id [4]=created-at [5]=ttl-ms
export const CAPTURE_GENERATION_LUA = `${NOW_FN}
local head_key, gens_key, tokens_key = KEYS[1], KEYS[2], KEYS[3]
local captures_key, expiries_key = KEYS[4], KEYS[5]
local now = tf_now(ARGV[1])
local inc, attempt_id = ARGV[2], ARGV[3]
local created_at, ttl = tonumber(ARGV[4]), tonumber(ARGV[5])

local expired = redis.call('ZRANGEBYSCORE', expiries_key, '-inf', now)
for _, id in ipairs(expired) do redis.call('HDEL', captures_key, id) end
if #expired > 0 then redis.call('ZREM', expiries_key, unpack(expired)) end

local current_token = redis.call('HGET', tokens_key, inc)
local head = tf_head(head_key, now)
local generation_live = head and head.state == 'open' and head.inc == inc
  and redis.call('SISMEMBER', gens_key, inc) == 1 and current_token ~= false
local raw = redis.call('HGET', captures_key, attempt_id)
if raw then
  local prior = cjson.decode(raw)
  if prior.inc ~= inc or prior.createdAt ~= created_at or prior.expiresAt <= now
    or not generation_live or prior.token ~= current_token then
    return cjson.encode({ rejected = true, terminal = true, reason = 'generation capture is invalid' })
  end
  prior.expiresAt = now + ttl
  redis.call('HSET', captures_key, attempt_id, cjson.encode(prior))
  redis.call('ZADD', expiries_key, prior.expiresAt, attempt_id)
  return cjson.encode({ ok = true, token = prior.token })
end

if created_at == nil or created_at ~= math.floor(created_at) or created_at > now or created_at + ttl <= now then
  return cjson.encode({ rejected = true, terminal = true, reason = 'generation capture attempt is absent or stale' })
end
if not generation_live then
  return cjson.encode({ rejected = true, terminal = true, reason = 'generation is not current and open' })
end
local record = { inc = inc, token = current_token, createdAt = created_at, expiresAt = now + ttl }
redis.call('HSET', captures_key, attempt_id, cjson.encode(record))
redis.call('ZADD', expiries_key, record.expiresAt, attempt_id)
return cjson.encode({ ok = true, token = current_token })
`

export const CAPTURE_GENERATION_CMD = 'tfRoomCaptureGeneration'
export const CAPTURE_GENERATION_KEYS = 5

// Post-SUBSCRIBE validation is exact and read-only on failure. Only a successful first establishment
// may touch its capture pin; a stale/delayed ack can never extend abandoned lifecycle state.
//   KEYS: same five keys as capture
//   ARGV: [1]=now [2]=inc [3]=expected-token [4]=attempt-id-or-empty [5]=created-at-or-empty [6]=ttl-ms
export const VALIDATE_GENERATION_LUA = `${NOW_FN}
local head_key, gens_key, tokens_key = KEYS[1], KEYS[2], KEYS[3]
local captures_key, expiries_key = KEYS[4], KEYS[5]
local now = tf_now(ARGV[1])
local inc, expected_token = ARGV[2], ARGV[3]
local head = tf_head(head_key, now)
local current_token = redis.call('HGET', tokens_key, inc)
if not head or head.state ~= 'open' or head.inc ~= inc
  or redis.call('SISMEMBER', gens_key, inc) ~= 1 or current_token ~= expected_token then
  return cjson.encode({ ok = false, terminal = true })
end
if ARGV[4] ~= '' then
  local raw = redis.call('HGET', captures_key, ARGV[4])
  if not raw then return cjson.encode({ ok = false, terminal = true }) end
  local capture = cjson.decode(raw)
  local created_at = tonumber(ARGV[5])
  if capture.inc ~= inc or capture.token ~= expected_token or capture.createdAt ~= created_at
    or capture.expiresAt <= now then
    return cjson.encode({ ok = false, terminal = true })
  end
  capture.expiresAt = now + tonumber(ARGV[6])
  redis.call('HSET', captures_key, ARGV[4], cjson.encode(capture))
  redis.call('ZADD', expiries_key, capture.expiresAt, ARGV[4])
end
return cjson.encode({ ok = true })
`

export const VALIDATE_GENERATION_CMD = 'tfRoomValidateGeneration'
export const VALIDATE_GENERATION_KEYS = 5

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
//   ARGV: [1]=now [2]=inc [3]=laneKind [4]=closingLease('') [5]=retain('0'|'1')
//         [6]=orderTtlMs('') [7]=payload [8]=aggregate retained payload cap
export const COMMIT_LUA = `${NOW_FN}
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

-- The counter is aggregate PAYLOAD bytes. A stored frame has a fixed 12-byte order header, excluded
-- when replacing an existing retained lane. The check precedes every acceptance mutation.
local retained_total = nil
if ARGV[5] == '1' then
  local current_total = tonumber(redis.call('GET', retained_size_key) or '0')
  local old_frame_bytes = redis.call('STRLEN', retained_key)
  local old_payload_bytes = 0
  if old_frame_bytes > 12 then old_payload_bytes = old_frame_bytes - 12 end
  retained_total = current_total - old_payload_bytes + string.len(ARGV[7])
  if retained_total > tonumber(ARGV[8]) then
    return redis.error_reply('commitLane: retained aggregate ' .. retained_total .. ' bytes exceeds the ' .. ARGV[8] .. ' byte cap')
  end
end

-- advance the lane's order domain: seq strictly increasing, timestamp clamped non-decreasing; a
-- logically-expired mark resets (matches the reference).
local base_seq, base_ts = 0, 0
local prev = redis.call('GET', order_key)
if prev then
  local pseq, pts, pexp = string.match(prev, '^(%d+):(%d+):(%d*)$')
  local expired = (pexp ~= '' and tonumber(pexp) <= now)
  if not expired then base_seq = tonumber(pseq); base_ts = tonumber(pts) end
end
local seq = base_seq + 1
local ts = now
if base_ts > ts then ts = base_ts end
local exp_str = ''
if ARGV[6] ~= '' then exp_str = tostring(now + tonumber(ARGV[6])) end
redis.call('SET', order_key, seq .. ':' .. ts .. ':' .. exp_str)
if ARGV[6] ~= '' then redis.call('PEXPIRE', order_key, tonumber(ARGV[6])) end
local ts_hi = math.floor(ts / 4294967296)
local ts_lo = ts - ts_hi * 4294967296
local frame = struct.pack('>I4I4I4', seq, ts_hi, ts_lo) .. ARGV[7]
if ARGV[5] == '1' then
  redis.call('SET', retained_key, frame)
  redis.call('SET', retained_size_key, retained_total)
end
local receivers = redis.call('PUBLISH', channel_key, frame)
return '{"accepted":true,"seq":' .. seq .. ',"timestamp":' .. ts .. ',"receivers":' .. receivers .. '}'
`

export const COMMIT_CMD = 'tfRoomCommit'
export const COMMIT_KEYS = 5

// Retained deletion updates the aggregate payload counter in the same atomic record as payload removal.
// KEYS[1] is the counter; KEYS[2..] are the selected retained lane keys.
export const RETAINED_DELETE_LUA = `
local size_key = KEYS[1]
local total = tonumber(redis.call('GET', size_key) or '0')
for i = 2, #KEYS do
  local frame_bytes = redis.call('STRLEN', KEYS[i])
  if frame_bytes > 0 then
    local payload_bytes = 0
    if frame_bytes > 12 then payload_bytes = frame_bytes - 12 end
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

// The 12-byte publish/retained header, shared by the commit Lua (`struct.pack('>I4I4I4', …)`) and the
// JS decoders. `ts` is split into two u32s to stay ms-accurate beyond ~50 days.
export const HEADER_BYTES = 12
export const U32_RANGE = 0x1_0000_0000

export function decodeFrameHeader(frame: Uint8Array): { seq: number; timestamp: number; payload: Uint8Array } {
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength)
  const seq = view.getUint32(0, false)
  const timestamp = view.getUint32(4, false) * U32_RANGE + view.getUint32(8, false)
  return { seq, timestamp, payload: frame.subarray(HEADER_BYTES) }
}
