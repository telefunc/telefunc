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
//   channels:  tf:room:{rid}:ch:<inc>:<laneKey>    PUBLISH/SUBSCRIBE — INC-SCOPED (an old-inc SUBSCRIBE
//                                                  can never hear a recreation — I11)
//   gens:      tf:room:{rid}:gens           SET of incs — SADD'd by the head-CX that installs an inc,
//                                           SREM'd by dropGeneration; the fresh-inc guard is one SISMEMBER
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
export function channelKey(prefix: string, roomId: string, inc: string, laneKey: string): string {
  return `${roomTag(prefix, roomId)}:ch:${inc}:${laneKey}`
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
//   KEYS: [1]=head [2]=gens [3]=headrev
//   ARGV: [1]=now [2]=cxJson{form,rev?,closingLease?} [3]=nextJson{kind,state?,inc?,config?,lease?,ttlMs?}
export const HEAD_CX_LUA = `${NOW_FN}
local head_key, gens_key, rev_key = KEYS[1], KEYS[2], KEYS[3]
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

-- operation legality of the tombstone delete is decided BEFORE any compare, so misuse throws even where
-- the compare would have conflicted (spi.md §2 — the delete row only).
if nx.kind == 'delete' then
  if from ~= 'closed' then
    return redis.error_reply("head CX: {delete} is legal only against a 'closed' tombstone, not '" .. from .. "'")
  end
  if (not cur) or cur.rev ~= cx.rev then return conflict() end
  redis.call('DEL', head_key)
  return '{"tag":"deleted"}'
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

-- transition-table legality (throws), validated against the head the compare matched
local transition = from .. ' + ' .. cx.form .. ' -> ' .. nx.state
if transition == 'absent + absent -> open' or transition == 'closed + generic -> open' then
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
if nx.inc ~= nil then redis.call('SADD', gens_key, nx.inc) end
return '{"tag":"head","head":' .. encoded .. '}'
`

export const HEAD_CX_CMD = 'tfRoomHeadCx'
export const HEAD_CX_KEYS = 3

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

// COMMIT — atomic acceptance: head precondition (one boolean, two branches), order advance, optional
// retained install, then PUBLISH (the broker handoff; `delivery` settles on this reply, receivers = its
// count). Supplying a closing lease selects the narrow closing-control branch, which is what makes every
// other lane stale while closing (I12).
//   KEYS: [1]=head [2]=order [3]=retained [4]=channel
//   ARGV: [1]=now [2]=inc [3]=laneKind [4]=closingLease('') [5]=retain('0'|'1') [6]=orderTtlMs('') [7]=payload
export const COMMIT_LUA = `${NOW_FN}
local head_key, order_key, retained_key, channel_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
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
if ARGV[5] == '1' then redis.call('SET', retained_key, frame) end
local receivers = redis.call('PUBLISH', channel_key, frame)
return '{"accepted":true,"seq":' .. seq .. ',"timestamp":' .. ts .. ',"receivers":' .. receivers .. '}'
`

export const COMMIT_CMD = 'tfRoomCommit'
export const COMMIT_KEYS = 4

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
