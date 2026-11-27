// The store's replies: how each wording a store replies with becomes Telefunc's `BackendError`, in one ordered table.
export { redisFailure, isRefusal, isLoginRefusal, orderRecordRefusal, staleStart }

import { BackendError } from 'telefunc/__internal'
import { isBug } from './assert.js'
import { getGlobalObject } from './getGlobalObject.js'
import {
  addressOf,
  ClusterNodesFailed,
  commandTimeoutOf,
  isCluster,
  isReplyError,
  isTimedOut,
  isUnsent,
  lastClusterReply,
  type RedisClient,
} from './ioredis.js'

type Code = BackendError['code']

/** A reply's wording the backend tells by its kind, whatever its first word. `notRun`: the reply, given at top level or
 *  by Redis 7 and later inside a script, proves the command didn't run. `stands`: it is `'denied'`, which stands until
 *  an operator changes something, so it ends a subscription (`isRefusal()`); what is `'unavailable'` passes, and a
 *  subscription waits it out. */
type ReplyRow = { kind: Kind; wording: RegExp; code: Code; notRun: boolean; stands: boolean }
type Kind = 'replica' | 'acl' | 'login' | 'unknown command'

/** In order: the first row whose wording starts the reply (unwrapped, see `IN_SCRIPT`) is the reply's. */
const REPLIES: readonly ReplyRow[] = [
  // A replica's refusal of a write, at top level or inside a script: a failover passes.
  { kind: 'replica', wording: /^READONLY /, code: 'unavailable', notRun: true, stands: false },
  // Its user's ACL, at top level or inside a script: Redis 7 and later word a script's ACL refusal as `ERR`. An ACL
  // refusal inside one of Telefunc's write scripts comes before its first write, as each script checks its calls first
  // (`tf_allowed`).
  {
    kind: 'acl',
    wording: /^(?:NOPERM |ERR ACL failure in script: |(?:ERR )?The user executing the script )/,
    code: 'denied',
    notRun: true,
    stands: true,
  },
  // A refused login: Redis 6 and later, and Redis 5's `AUTH`.
  { kind: 'login', wording: /^(?:WRONGPASS|NOAUTH) /, code: 'denied', notRun: true, stands: true },
  { kind: 'login', wording: /^ERR invalid password/, code: 'denied', notRun: false, stands: true },
  // A command Redis doesn't know: disabled or renamed by its operator.
  { kind: 'unknown command', wording: /^ERR unknown command /, code: 'denied', notRun: true, stands: true },
  { kind: 'unknown command', wording: /^ERR unknown subcommand /, code: 'denied', notRun: false, stands: true },
]

/** A reply told by its code, its first word. `notRun`: at top level, or raised by Redis 7 and later inside a script,
 *  which they do only until the script writes. A code that isn't here is `'failed'` and may have run. */
const CODES: Readonly<Record<string, { code: Code; notRun: boolean; toDo?: string }>> = {
  BUSY: {
    code: 'unavailable',
    notRun: true,
    toDo: "another client's script keeps Redis busy; end it with SCRIPT KILL",
  },
  LOADING: {
    code: 'unavailable',
    notRun: true,
    toDo: 'Redis is loading its data; the backend can use it once that ends',
  },
  NOREPLICAS: {
    code: 'unavailable',
    notRun: true,
    toDo: 'fewer replicas than its min-replicas-to-write have caught up; a later call may succeed once they do',
  },
  OOM: {
    code: 'unavailable',
    notRun: true,
    toDo: 'Redis reached its maxmemory; a later call may succeed as keys expire, and if it lasts, raise maxmemory, with maxmemory-policy noeviction',
  },
  MISCONF: { code: 'failed', notRun: true },
  MASTERDOWN: { code: 'unavailable', notRun: false },
  TRYAGAIN: { code: 'unavailable', notRun: false },
  CLUSTERDOWN: { code: 'unavailable', notRun: false },
  WRONGTYPE: {
    code: 'failed',
    notRun: false,
    toDo: "a key under Telefunc's prefix holds a value Telefunc didn't write; keep that prefix to Telefunc",
  },
}

/** How Redis 5 and 6 and Dragonfly wrap the reply to a call a script makes; such a reply proves nothing about running. */
const IN_SCRIPT = /^ERR Error running script \(call to (?:f_)?[0-9a-f]+\): @user_script:\d+: (?:@user_script: \d+: )?-?/

/** A Cluster's redirection, which ioredis follows until its redirections run out. */
const REDIRECTION = /^(MOVED|ASK) /
/** A node's answer while the command's slot is busy, which names no other node: ioredis tries again until its
 *  redirections run out. */
const BUSY_SLOT = /^TRYAGAIN /

/** A write script's guard's refusal before its first write (`refuseLua`, commands.ts), by its word: WRONGTYPE for a
 *  key of another type, ERR for a value, an order record too. Only the guard puts TFNOTRUN at the start of a reply. It
 *  stands until an operator changes the key. */
const GUARD_REFUSAL = /^TFNOTRUN (WRONGTYPE|ERR) /

/** A subscription's start script's refusal before anything else, as its user's ACL denies the `INFO` it reads first. */
const TF_INFO = /^TFINFO (.+)$/

/** What the backend reports of an error ioredis gave on a connection of `redis`'s, for a command that may write
 *  (`writes`) or one that can't: the one place a failure of Redis becomes Telefunc's `BackendError`, which says what
 *  failed and what to do, with Redis's error code (the first word of its reply) or the connection's, and holds nothing
 *  of ioredis' error: not the command, its arguments (a password, a message's bytes) or a nested error. A command that
 *  can't write changed nothing; one that may write didn't run only where ioredis never wrote it or the reply proves
 *  it. An error the backend's own `assert()` threw passes as it is, and so does one this made. */
function redisFailure(error: unknown, redis: RedisClient, writes: boolean): unknown {
  if (error instanceof BackendError || isBug(error)) return error
  if (error instanceof ClusterNodesFailed) {
    // A refusal stands for the Cluster once every node it tried refused: a node that is only unreachable may let the
    // backend in once it is back.
    const failures = error.nodeErrors.map((nodeError) => redisFailure(nodeError, redis, writes))
    const [first] = failures
    if (first !== undefined && failures.every(isRefusal)) return first
  }
  const where = addressOf(redis)
  const outcome = (proof: boolean): BackendError['outcome'] => (!writes || proof ? 'not-run' : 'may-have-run')
  // A Cluster command ioredis gave up is told by the reply that ended it, as if that reply had come on its own: every
  // try before it was a redirection, MOVED, ASK, TRYAGAIN or CLUSTERDOWN, which ran nothing, as ioredis tries a command
  // again on those only and counts its redirections down before it reads a reply, so at an app's `maxRedirections: 0`
  // the first reply, whatever it is, ends it.
  const last = lastClusterReply(error)
  const redirection = last === undefined ? undefined : REDIRECTION.exec(last)?.[1]
  if (redirection !== undefined) {
    // The reply says the node doesn't serve the slot now, not why: a reshard and a failover both answer it.
    return new BackendError({
      code: 'unavailable',
      outcome: 'not-run',
      message:
        `The Redis Cluster at ${where} redirected a command of Telefunc's Redis backend (${redirection}) more times ` +
        "than ioredis follows, so it didn't run: a later call may succeed once ioredis reaches the node that serves " +
        "the command's slot",
    })
  }
  if (last !== undefined && BUSY_SLOT.test(last)) {
    return new BackendError({
      code: 'unavailable',
      outcome: 'not-run',
      message: `The Redis Cluster at ${where} answered a command of Telefunc's Redis backend with TRYAGAIN more times than ioredis tries again, as the command's slot was busy, so it didn't run: a later call may succeed`,
    })
  }
  const message = last ?? (isReplyError(error) ? error.message : undefined)
  if (message === undefined && isTimedOut(error)) {
    return new BackendError({
      code: 'unreachable',
      outcome: outcome(false),
      message: `Redis at ${where} didn't answer a command of Telefunc's Redis backend within ${commandTimeoutOf(redis)} ms: a later call may succeed once it answers`,
    })
  }
  if (message === undefined) {
    const code = connectionCode(error)
    return new BackendError({
      code: 'unreachable',
      outcome: outcome(isUnsent(error)),
      message: `Telefunc's Redis backend can't reach Redis at ${where}${code === undefined ? '' : ` (${code})`}`,
    })
  }
  const info = TF_INFO.exec(message)?.[1]
  if (info !== undefined) {
    const failure = new BackendError({
      code: 'denied',
      outcome: outcome(true),
      message:
        `Redis at ${where} refused Telefunc's Redis backend ${info} by its user's ACL, which a subscription's start ` +
        'reads: allow that user +info, and +cluster|info on a Cluster, as the ACL rule in the docs of @telefunc/redis lists',
    })
    refusals.set(failure, 'acl')
    return failure
  }
  if (message.startsWith('TFSTALE ')) return staleStart(where)
  const guarded = GUARD_REFUSAL.exec(message)
  if (guarded !== null) {
    // In Telefunc's own words, as for every reply: the reply's text names the key.
    const [word, held] =
      guarded[1] === 'WRONGTYPE'
        ? ['WRONGTYPE', 'a key of another type than Telefunc keeps there']
        : ['ERR', "a value Telefunc didn't write"]
    const failure = new BackendError({
      code: 'failed',
      outcome: outcome(true),
      message:
        `Redis at ${where} holds, under Telefunc's prefix, ${held}, so Telefunc's Redis backend changed nothing ` +
        `(${word}): keep that prefix to Telefunc`,
    })
    refusals.set(failure, 'guard')
    return failure
  }
  const wrapper = IN_SCRIPT.exec(message)
  const reply = wrapper === null ? message : message.slice(wrapper[0].length)
  const shown = [reply, message].map((text) => text.split(' ', 1)[0]).find((word) => /^[A-Z][A-Z_]*$/.test(word ?? ''))
  const tagged = shown === undefined ? '' : ` (${shown})`
  const row = REPLIES.find(({ wording }) => wording.test(reply))
  if (row !== undefined) {
    const failure = new BackendError({
      code: row.code,
      outcome: outcome(row.notRun && wrapper === null),
      message: sentence(row.kind, where, tagged, isCluster(redis)),
    })
    if (row.stands) refusals.set(failure, row.kind)
    return failure
  }
  const known = shown === undefined ? undefined : CODES[shown]
  return new BackendError({
    code: known?.code ?? 'failed',
    outcome: outcome(known !== undefined && known.notRun && wrapper === null),
    message:
      `Redis at ${where} failed a command of Telefunc's Redis backend${tagged}` +
      (known?.toDo === undefined ? ": Redis's log may say why" : `: ${known.toDo}`),
  })
}

/** A record at `key`, read for a subscription's start, that the backend can't go on from: another client wrote it, or
 *  its next count would pass 2^53 - 1. A script refuses it as the guard does; read, it stands until an operator changes
 *  the key, as a refusal does. */
function orderRecordRefusal(key: string): BackendError {
  const failure = new BackendError({
    code: 'failed',
    outcome: 'not-run',
    message:
      `Telefunc's Redis backend can't go on from the order record at ${key}: it isn't one Telefunc writes, or its next ` +
      "count would pass 2^53 - 1; keep Telefunc's prefix to Telefunc",
  })
  refusals.set(failure, 'order record')
  return failure
}

/** A subscription's start read from a replica, or across two histories: it is read again, once the primary answers. */
function staleStart(where: string): BackendError {
  return new BackendError({
    code: 'unavailable',
    outcome: 'not-run',
    message:
      `Telefunc's Redis backend read a subscription's start at ${where} from a replica or across a change of Redis's ` +
      'history: it reads it again',
  })
}

/** A reply's sentence, by its kind. A Cluster is named by the startup nodes the app gave (`addressOf()`), as the reply
 *  doesn't say which node sent it. */
function sentence(kind: Kind, where: string, tagged: string, cluster: boolean): string {
  switch (kind) {
    case 'replica':
      // A Cluster client reaches each slot's primary by itself: ioredis hands a write to a replica only while it holds
      // no connection to the slot's primary, so no other client would help.
      // TODO: remove once the ioredis peer range starts at 6.1.0, which fixed https://github.com/redis/ioredis/issues/2274
      if (cluster) {
        return (
          `A replica of the Redis Cluster at ${where} refused a write of Telefunc's Redis backend${tagged}, as ioredis ` +
          "sent it there while it held no connection to the slot's primary: a later call reaches the primary once " +
          "ioredis reads the Cluster's slots again"
        )
      }
      return (
        `Telefunc's Redis backend reached a replica at ${where}, which refuses writes${tagged}: give installRedis() a ` +
        'client of the primary'
      )
    case 'acl':
      return (
        `Redis at ${where} refused Telefunc's Redis backend a command, key or channel by its user's ACL${tagged}: ` +
        'allow that user what the ACL rule in the docs of @telefunc/redis lists'
      )
    case 'login':
      return (
        `Redis at ${where} refused the login of Telefunc's Redis backend${tagged}: check the username and password ` +
        'of the client given to installRedis()'
      )
    case 'unknown command':
      return (
        `Redis at ${where} doesn't run a command Telefunc's Redis backend sends, as its operator disabled or renamed ` +
        'it: allow the commands the ACL rule in the docs of @telefunc/redis lists'
      )
  }
}

/** The system's code of a failed connection, such as `ECONNREFUSED`. */
function connectionCode(error: unknown): string | undefined {
  if (!(error instanceof Error) || !('code' in error) || typeof error.code !== 'string') return undefined
  return /^E[A-Z]+$/.test(error.code) ? error.code : undefined
}

/** A refusal that stands until an operator changes something: a row of `REPLIES` that `stands`, an ACL refusal, a
 *  refused login, a command Redis doesn't run or an order record Telefunc can't go on from; `READONLY`, `BUSY`,
 *  `LOADING`, a stale start and the like pass. */
function isRefusal(error: unknown): error is BackendError {
  return error instanceof BackendError && refusals.has(error)
}

/** A refused login (`WRONGPASS`, `NOAUTH`, Redis 5's `ERR invalid password`), told from an ACL's `NOPERM` by the
 *  reply's first word, which Redis 7.0.0 and 7.4.12 and Valkey 7.2.5 and 8.1.1 word alike. A login is each node's
 *  own; an ACL refusal is of one command, key or channel. */
function isLoginRefusal(error: unknown): error is BackendError {
  return error instanceof BackendError && refusals.get(error) === 'login'
}

/** This file's state on `globalThis`. */
function createGlobalObject() {
  return {
    /** The failures that are a refusal that stands, with their reply's kind (`isRefusal()`, `isLoginRefusal()`). */
    refusals: new WeakMap<BackendError, Kind | 'guard' | 'order record'>(),
  }
}
declare module './getGlobalObject.js' {
  interface GlobalObjects {
    'redisFailure.ts': ReturnType<typeof createGlobalObject>
  }
}
const { refusals } = getGlobalObject('redisFailure.ts', createGlobalObject)
