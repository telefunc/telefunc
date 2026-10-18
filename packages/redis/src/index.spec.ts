import { Cluster, Redis } from 'ioredis'
import { expect, onTestFinished, test } from 'vitest'
import { installRedis } from './index.js'
import { disposeBackend } from '../../telefunc/dist/wire-protocol/backend/install.js'

const DIFFERENT = /a different backend is already installed/

test('a client built again for the same Redis keeps the installed backend, and one for another Redis throws', () => {
  onTestFinished(() => disposeBackend())
  const client = (url: string) => new Redis(url, { lazyConnect: true })
  installRedis(client('redis://127.0.0.1:41300/0'))
  expect(() => installRedis(client('redis://127.0.0.1:41300/0'))).not.toThrow()
  expect(() => installRedis(client('redis://127.0.0.1:41301/0'))).toThrow(DIFFERENT)
  expect(() => installRedis(client('redis://127.0.0.1:41300/1'))).toThrow(DIFFERENT)
  expect(() => installRedis(client('redis://127.0.0.1:41300/0'), { prefix: 'app:' })).toThrow(DIFFERENT)
})

test('a Cluster client built again for the same startup nodes keeps the installed backend, and one for other nodes throws', () => {
  onTestFinished(() => disposeBackend())
  const cluster = (port: number) => new Cluster([{ host: '127.0.0.1', port }], { lazyConnect: true })
  installRedis(cluster(41300))
  expect(() => installRedis(cluster(41300))).not.toThrow()
  expect(() => installRedis(cluster(41301))).toThrow(DIFFERENT)
  expect(() => installRedis(new Redis('redis://127.0.0.1:41300', { lazyConnect: true }))).toThrow(DIFFERENT)
})
