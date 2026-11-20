// Every wording of a store's reply the backend reads, as the backend reports it: its code, its outcome for a command
// that may write (one that can't write is always 'not-run'), whether it is a refusal that stands, and its sentence.
// Written against the backend as it was before its replies moved into one table, so the table keeps each answer.
import { Redis } from 'ioredis'
import { afterAll, describe, expect, it } from 'vitest'
import { isRefusal, redisFailure } from './redisFailure.js'

const redis = new Redis({ lazyConnect: true, host: '127.0.0.1', port: 6390 })
afterAll(() => redis.disconnect())

/** A reply as ioredis gives it. */
function reply(message: string): Error {
  const error = new Error(message)
  error.name = 'ReplyError'
  return error
}

type Row = [reply: string, code: string, outcome: 'not-run' | 'may-have-run', refusal: boolean, sentence: string]

const ROWS: Row[] = [
  [
    "NOPERM this user has no permissions to run the 'get' command or its subcommand",
    'denied',
    'not-run',
    true,
    "Redis at 127.0.0.1:6390 refused Telefunc's Redis backend a command, key or channel by its user's ACL (NOPERM): allow that user what the ACL rule in the docs of @telefunc/redis lists",
  ],
  [
    'NOPERM this user has no permissions to access one of the channels used as arguments',
    'denied',
    'not-run',
    true,
    "Redis at 127.0.0.1:6390 refused Telefunc's Redis backend a command, key or channel by its user's ACL (NOPERM): allow that user what the ACL rule in the docs of @telefunc/redis lists",
  ],
  [
    "NOPERM User probe has no permissions to run the 'get' command",
    'denied',
    'not-run',
    true,
    "Redis at 127.0.0.1:6390 refused Telefunc's Redis backend a command, key or channel by its user's ACL (NOPERM): allow that user what the ACL rule in the docs of @telefunc/redis lists",
  ],
  [
    'NOPERM No permissions to access a key',
    'denied',
    'not-run',
    true,
    "Redis at 127.0.0.1:6390 refused Telefunc's Redis backend a command, key or channel by its user's ACL (NOPERM): allow that user what the ACL rule in the docs of @telefunc/redis lists",
  ],
  [
    "ERR Error running script (call to f_d3c21d0c2b9ca22f82737626a27bcaf5d288f99f): @user_script:1: @user_script: 1: The user executing the script can't run this command or subcommand",
    'denied',
    'may-have-run',
    true,
    "Redis at 127.0.0.1:6390 refused Telefunc's Redis backend a command, key or channel by its user's ACL (ERR): allow that user what the ACL rule in the docs of @telefunc/redis lists",
  ],
  [
    "ERR Error running script (call to f_9ddb5d56e1d5c3a3b21c1aad774273186b4a6d34): @user_script:1: @user_script: 1: The user executing the script can't publish to the channel mentioned in the command",
    'denied',
    'may-have-run',
    true,
    "Redis at 127.0.0.1:6390 refused Telefunc's Redis backend a command, key or channel by its user's ACL (ERR): allow that user what the ACL rule in the docs of @telefunc/redis lists",
  ],
  [
    "ERR The user executing the script can't run this command or subcommand script: d3c21d0c2b9ca22f82737626a27bcaf5d288f99f, on @user_script:1.",
    'denied',
    'not-run',
    true,
    "Redis at 127.0.0.1:6390 refused Telefunc's Redis backend a command, key or channel by its user's ACL (ERR): allow that user what the ACL rule in the docs of @telefunc/redis lists",
  ],
  [
    "ERR ACL failure in script: User probe has no permissions to run the 'get' command script: d3c21d0c2b9ca22f82737626a27bcaf5d288f99f, on @user_script:1.",
    'denied',
    'not-run',
    true,
    "Redis at 127.0.0.1:6390 refused Telefunc's Redis backend a command, key or channel by its user's ACL (ERR): allow that user what the ACL rule in the docs of @telefunc/redis lists",
  ],
  [
    'ERR ACL failure in script: No permissions to access a channel script: 9ddb5d56e1d5c3a3b21c1aad774273186b4a6d34, on @user_script:1.',
    'denied',
    'not-run',
    true,
    "Redis at 127.0.0.1:6390 refused Telefunc's Redis backend a command, key or channel by its user's ACL (ERR): allow that user what the ACL rule in the docs of @telefunc/redis lists",
  ],
  [
    "READONLY You can't write against a read only replica.",
    'unavailable',
    'not-run',
    false,
    "Telefunc's Redis backend reached a replica at 127.0.0.1:6390, which refuses writes (READONLY): give installRedis() a client of the primary",
  ],
  [
    "READONLY You can't write against a read only replica. script: bd6f3018b28bbdb36260f19d3c44afed740db987, on @user_script:1.",
    'unavailable',
    'not-run',
    false,
    "Telefunc's Redis backend reached a replica at 127.0.0.1:6390, which refuses writes (READONLY): give installRedis() a client of the primary",
  ],
  [
    "ERR Error running script (call to f_bd6f3018b28bbdb36260f19d3c44afed740db987): @user_script:1: @user_script: 1: -READONLY You can't write against a read only replica.",
    'unavailable',
    'may-have-run',
    false,
    "Telefunc's Redis backend reached a replica at 127.0.0.1:6390, which refuses writes (READONLY): give installRedis() a client of the primary",
  ],
  [
    'ERR Error running script (call to ee33e8171bc4a2b477d56ebe06c4d9545f5b840a): @user_script:2: -NOPERM probe has no ACL permissions',
    'denied',
    'may-have-run',
    true,
    "Redis at 127.0.0.1:6390 refused Telefunc's Redis backend a command, key or channel by its user's ACL (NOPERM): allow that user what the ACL rule in the docs of @telefunc/redis lists",
  ],
  [
    'WRONGPASS invalid username-password pair or user is disabled.',
    'denied',
    'not-run',
    true,
    "Redis at 127.0.0.1:6390 refused the login of Telefunc's Redis backend (WRONGPASS): check the username and password of the client given to installRedis()",
  ],
  [
    'NOAUTH Authentication required.',
    'denied',
    'not-run',
    true,
    "Redis at 127.0.0.1:6390 refused the login of Telefunc's Redis backend (NOAUTH): check the username and password of the client given to installRedis()",
  ],
  [
    'ERR invalid password',
    'denied',
    'may-have-run',
    true,
    "Redis at 127.0.0.1:6390 refused the login of Telefunc's Redis backend (ERR): check the username and password of the client given to installRedis()",
  ],
  [
    "ERR unknown command 'evalsha', with args beginning with: ",
    'denied',
    'not-run',
    true,
    "Redis at 127.0.0.1:6390 doesn't run a command Telefunc's Redis backend sends, as its operator disabled or renamed it: allow the commands the ACL rule in the docs of @telefunc/redis lists",
  ],
  [
    "ERR unknown subcommand 'setinfo'. Try CLIENT HELP.",
    'denied',
    'may-have-run',
    true,
    "Redis at 127.0.0.1:6390 doesn't run a command Telefunc's Redis backend sends, as its operator disabled or renamed it: allow the commands the ACL rule in the docs of @telefunc/redis lists",
  ],
  [
    'LOADING Redis is loading the dataset in memory',
    'unavailable',
    'not-run',
    false,
    "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (LOADING): Redis is loading its data; the backend can use it once that ends",
  ],
  [
    'BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSCRIPT.',
    'unavailable',
    'not-run',
    false,
    "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (BUSY): another client's script keeps Redis busy; end it with SCRIPT KILL",
  ],
  [
    "MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.",
    'unavailable',
    'may-have-run',
    false,
    "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (MASTERDOWN): Redis's log may say why",
  ],
  [
    'TRYAGAIN Multiple keys request during rehashing of slot',
    'unavailable',
    'may-have-run',
    false,
    "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (TRYAGAIN): Redis's log may say why",
  ],
  [
    'CLUSTERDOWN The cluster is down',
    'unavailable',
    'may-have-run',
    false,
    "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (CLUSTERDOWN): Redis's log may say why",
  ],
  [
    'NOREPLICAS Not enough good replicas to write.',
    'unavailable',
    'not-run',
    false,
    "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (NOREPLICAS): fewer replicas than its min-replicas-to-write have caught up; a later call may succeed once they do",
  ],
  [
    "OOM command not allowed when used memory > 'maxmemory'.",
    'unavailable',
    'not-run',
    false,
    "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (OOM): Redis reached its maxmemory; a later call may succeed as keys expire, and if it lasts, raise maxmemory, with maxmemory-policy noeviction",
  ],
  [
    "OOM command not allowed when used memory > 'maxmemory'. script: 06f79f6ab4, on @user_script:1.",
    'unavailable',
    'not-run',
    false,
    "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (OOM): Redis reached its maxmemory; a later call may succeed as keys expire, and if it lasts, raise maxmemory, with maxmemory-policy noeviction",
  ],
  [
    "ERR Error running script (call to f_06f79f6ab4): @user_script:1: -OOM command not allowed when used memory > 'maxmemory'.",
    'unavailable',
    'may-have-run',
    false,
    "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (OOM): Redis reached its maxmemory; a later call may succeed as keys expire, and if it lasts, raise maxmemory, with maxmemory-policy noeviction",
  ],
  [
    "MISCONF Redis is configured to save RDB snapshots, but it's currently unable to persist to disk.",
    'failed',
    'not-run',
    false,
    "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (MISCONF): Redis's log may say why",
  ],
  [
    'NOREPLICAS Not enough good replicas to write. script: 06f79f6ab4, on @user_script:1.',
    'unavailable',
    'not-run',
    false,
    "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (NOREPLICAS): fewer replicas than its min-replicas-to-write have caught up; a later call may succeed once they do",
  ],
  [
    'WRONGTYPE Operation against a key holding the wrong kind of value',
    'failed',
    'may-have-run',
    false,
    "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (WRONGTYPE): a key under Telefunc's prefix holds a value Telefunc didn't write; keep that prefix to Telefunc",
  ],
  [
    'WRONGTYPE Operation against a key holding the wrong kind of value script: 06f79f6ab4, on @user_script:1.',
    'failed',
    'may-have-run',
    false,
    "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (WRONGTYPE): a key under Telefunc's prefix holds a value Telefunc didn't write; keep that prefix to Telefunc",
  ],
  [
    'NOSCRIPT No matching script. Please use EVAL.',
    'failed',
    'may-have-run',
    false,
    "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (NOSCRIPT): Redis's log may say why",
  ],
  [
    'ERR user_script:1: Script attempted to access a non local key in a cluster node script: 06f79f6ab4, on @user_script:1.',
    'failed',
    'may-have-run',
    false,
    "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (ERR): Redis's log may say why",
  ],
  [
    'TFNOTRUN WRONGTYPE tf:0:o:k holds hash script: 06f79f6ab4, on @user_script:1.',
    'failed',
    'not-run',
    false,
    "Redis at 127.0.0.1:6390 holds, under Telefunc's prefix, a key of another type than Telefunc keeps there, so Telefunc's Redis backend changed nothing (WRONGTYPE): keep that prefix to Telefunc",
  ],
  [
    'TFNOTRUN ERR tf:0:h:r holds no JSON script: 06f79f6ab4, on @user_script:1.',
    'failed',
    'not-run',
    false,
    "Redis at 127.0.0.1:6390 holds, under Telefunc's prefix, a value Telefunc didn't write, so Telefunc's Redis backend changed nothing (ERR): keep that prefix to Telefunc",
  ],
  [
    'ERR user_script:1: TFNOTRUN WRONGTYPE forged script: 06f79f6ab4, on @user_script:1.',
    'failed',
    'may-have-run',
    false,
    "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (ERR): Redis's log may say why",
  ],
]

describe("a store's reply, as the backend reports it", () => {
  it.each(ROWS)('%s', (message, code, outcome, refusal, sentence) => {
    const written = redisFailure(reply(message), redis, true)
    const unwritten = redisFailure(reply(message), redis, false)
    expect(written).toMatchObject({ name: 'BackendError', code, outcome, message: sentence })
    expect(unwritten).toMatchObject({ name: 'BackendError', code, outcome: 'not-run', message: sentence })
    expect([isRefusal(written), isRefusal(unwritten)]).toEqual([refusal, refusal])
  })
})
