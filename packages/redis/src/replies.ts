// A reply as the backend reads it: each reader asserts the shape its command or script answers with, so a reply of
// another shape is a bug told where it is read.
export { bytes, text, number, list, texts, bytesList, twoOf, pairs, triples, json }

import { assert } from './assert.js'

function bytes(reply: unknown): Buffer {
  assert(Buffer.isBuffer(reply), 'A Redis reply is not bytes')
  return reply
}

function text(reply: unknown): string {
  assert(typeof reply === 'string', 'A Redis reply is not text')
  return reply
}

/** An integer reply, or one a shared client with ioredis's `stringNumbers` returns as a decimal string. */
function number(reply: unknown): number {
  const value = typeof reply === 'string' ? Number(reply) : reply
  assert(typeof value === 'number' && Number.isFinite(value), 'A Redis reply is not a number')
  return value
}

function list(reply: unknown): unknown[] {
  assert(Array.isArray(reply), 'A Redis reply is not an array')
  return reply
}

function texts(reply: unknown): string[] {
  return list(reply).map(text)
}

function bytesList(reply: unknown): Buffer[] {
  return list(reply).map(bytes)
}

/** The two items of `items`, which holds no other. */
function twoOf<T>(items: readonly T[]): [T, T] {
  const [first, second, ...rest] = items
  assert(first !== undefined && second !== undefined && rest.length === 0, 'A Redis reply is not a pair')
  return [first, second]
}

/** `items` two at a time. */
function pairs<T>(items: readonly T[]): [T, T][] {
  assert(items.length % 2 === 0, 'A Redis reply has an odd number of items')
  const out: [T, T][] = []
  for (let i = 0; i < items.length; i += 2) {
    const [first, second] = items.slice(i, i + 2)
    assert(first !== undefined && second !== undefined)
    out.push([first, second])
  }
  return out
}

/** `items` three at a time. */
function triples<T>(items: readonly T[]): [T, T, T][] {
  assert(items.length % 3 === 0, 'A Redis reply has a number of items that is not a multiple of three')
  const out: [T, T, T][] = []
  for (let i = 0; i < items.length; i += 3) {
    const [first, second, third] = items.slice(i, i + 3)
    assert(first !== undefined && second !== undefined && third !== undefined)
    out.push([first, second, third])
  }
  return out
}

/** A script's JSON reply, its shape still to be read. */
function json(reply: unknown): unknown {
  const value: unknown = JSON.parse(text(reply))
  return value
}
