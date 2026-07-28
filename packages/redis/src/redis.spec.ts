import type { Redis } from 'ioredis'
import { disposeBackend, getBackend, installBackend } from 'telefunc/backend'
import { afterEach, describe, expect, it } from 'vitest'
import { installRedis, RedisRoomBackend } from './index.js'

describe('Redis public installation', () => {
  afterEach(async () => {
    await disposeBackend()
  })

  it('installs one complete default, stays idempotent, and never overrides an explicit backend', () => {
    const redis = new ConstructionOnlyRedis()
    installRedis(redis as unknown as Redis, { prefix: 'install:' })
    const automatic = getBackend()
    expect(automatic.spiVersion).toBe(1)
    expect(redis.commands).toContain('tfPublish')
    expect(redis.commands).toContain('tfRoomCommit')

    installRedis(redis as unknown as Redis, { prefix: 'install:' })
    expect(getBackend()).toBe(automatic)

    const explicit = installBackend(
      () => new RedisRoomBackend({ redis: new ConstructionOnlyRedis() as unknown as Redis }),
    )
    installRedis(new ConstructionOnlyRedis() as unknown as Redis)
    expect(getBackend()).toBe(explicit)
  })
})

class ConstructionOnlyRedis {
  readonly commands: string[] = []
  defineCommand(name: string): void {
    this.commands.push(name)
  }
}
