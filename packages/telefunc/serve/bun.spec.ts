import { afterEach, expect, test, vi } from 'vitest'
import { Telefunc } from './bun.js'

afterEach(() => {
  vi.unstubAllGlobals()
})

test("Bun's WebSocket handler queues every frame instead of dropping past its default 16 MiB limit", () => {
  vi.stubGlobal('Bun', {}) // crossws's Bun adapter checks it runs on Bun
  expect(new Telefunc().websocket).toMatchObject({ backpressureLimit: 0 })
})
