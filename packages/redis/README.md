# `@telefunc/redis`

Redis-backed broadcast fan-out and Room state for Telefunc: one setup call makes both work across instances.

## Install

```sh
npm install @telefunc/redis ioredis
```

## Setup

```ts
import IORedis from 'ioredis'
import { installRedis } from '@telefunc/redis'

const redis = new IORedis('redis://localhost:6379', { maxRetriesPerRequest: 0 })
installRedis(redis)
```

That one `installRedis()` call configures Broadcast and Room from the same client. Never-resend options make a lost command reply reject rather than execute twice.

### Required client options

`installRedis()` rejects a client configured otherwise:

| Client | Required |
|---|---|
| `Redis` | `maxRetriesPerRequest: 0`; no `reconnectOnError`; no `keyPrefix` |
| `Cluster` | `retryDelayOnFailover: 0`; `redisOptions.maxRetriesPerRequest: 0`; no `redisOptions.reconnectOnError`; no `redisOptions.keyPrefix`; `scaleReads: 'master'` (the default) |

ioredis applies `keyPrefix` to commands but not to Pub/Sub channels; use `installRedis(redis, { prefix })` instead.

## Room storage

`installRedis()` accepts an ioredis `Redis` or `Cluster` client. On a Cluster, a room's keys share one hash slot, and replica or custom read routing is rejected, since Room reads must be consistent.

All subscriptions share one subscriber connection. When it drops, they resume on a fresh one; frames published in between are lost. Delivery stays at-most-once while a Cluster reshards: a frame that arrives after a newer one is dropped, never replayed, so callbacks never go back in order. A failover whose new master missed the last writes rewinds their sequence numbers, so subscribers still connected drop as many later frames as it lost. Keep master clocks synchronized: expiries use the clock of the master that owns the room's slot.

On a Cluster, a publish's `receivers` is omitted: a master's `PUBLISH` counts only its own subscribers, so it can't prove that nobody is subscribed.

```ts
import { Cluster } from 'ioredis'
import { installRedis } from '@telefunc/redis'

const redis = new Cluster([
  { host: '127.0.0.1', port: 7000 },
  { host: '127.0.0.1', port: 7001 },
  { host: '127.0.0.1', port: 7002 },
], { retryDelayOnFailover: 0, redisOptions: { maxRetriesPerRequest: 0 } })
installRedis(redis)
```

`installRedis()` uses the same optional `prefix` for Broadcast and Room; `{` is reserved in prefixes. Repeating the same client/prefix is idempotent for Room.

`Channel` is per-instance — reconnects must land on the instance holding the channel's state. Pair this package with sticky sessions at the load balancer; see [Scaling](https://telefunc.com/stream/scale).

### Sharing an existing client

Pass an [`ioredis`](https://github.com/redis/ioredis) instance to share TLS/authentication settings. Keep the never-resend settings above; installation rejects retry-capable clients:

```ts
import IORedis from 'ioredis'
import { installRedis } from '@telefunc/redis'

const redis = new IORedis(process.env.REDIS_URL, { tls: {}, maxRetriesPerRequest: 0 })
installRedis(redis)
```
