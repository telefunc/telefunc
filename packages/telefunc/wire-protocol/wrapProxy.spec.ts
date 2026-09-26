import { expect, it } from 'vitest'
import { wrapProxy } from './wrapProxy.js'

it('a proxied method keeps its identity across reads, as useSyncExternalStore requires', () => {
  const target = { onChange: (callback: () => void) => callback }
  const proxy = wrapProxy(target)
  expect(proxy.onChange).toBe(proxy.onChange)
})

it('a method read off the proxy keeps its receiver when called detached', () => {
  class Counter {
    count = 1
    read() {
      return this.count
    }
  }
  const { read } = wrapProxy(new Counter())
  expect(read()).toBe(1)
})
