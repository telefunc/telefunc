import { describe, expect, it } from 'vitest'
import { binaryData, binaryPayload, isLossy } from './broadcastPayload.js'

describe('a binary broadcast payload', () => {
  it.each([
    ['plain', [0xab, 1], [0xab, 1]],
    ['plain', [0x00], [0x00]],
    ['plain', [], []],
    ['plain', [0xfd, 1], [0xfd, 0xfd, 1]],
    ['plain', [0xfe, 1], [0xfd, 0xfe, 1]],
    ['lossy', [0xab, 1], [0xfe, 0xab, 1]],
    ['lossy', [], [0xfe]],
    ['lossy', [0xfd, 1], [0xfe, 0xfd, 1]],
    ['lossy', [0xfe, 1], [0xfe, 0xfe, 1]],
  ])('carries %s %j as %j, and reads back what was published', (kind, published, carried) => {
    const lossy = kind === 'lossy'
    const payload = binaryPayload(new Uint8Array(published), lossy)

    expect([...payload]).toEqual(carried)
    expect(isLossy(payload)).toBe(lossy)
    expect([...binaryData(payload)]).toEqual(published)
  })

  it.each([[[0xab, 1]], [[0x00]], [[]]])("is the caller's own %j when plain and unmarked", (published) => {
    const data = new Uint8Array(published)

    expect(binaryPayload(data)).toBe(data)
    expect(binaryData(data)).toBe(data)
  })
})
