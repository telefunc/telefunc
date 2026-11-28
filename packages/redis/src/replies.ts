// A reply as the backend reads it: each reader asserts the shape its command or script answers with, so a reply of
// another shape is a bug told where it is read. They read a reply where it lies, building only the value they return.
export { bytes, text, number, list, texts, bytesList, twoOf, eachPair, eachTriple, json }

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
  const items = list(reply)
  assert(isTexts(items), 'A Redis reply is not a list of text')
  return items
}

function isTexts(items: unknown[]): items is string[] {
  for (const item of items) if (typeof item !== 'string') return false
  return true
}

function bytesList(reply: unknown): Buffer[] {
  const items = list(reply)
  assert(isBytesList(items), 'A Redis reply is not a list of bytes')
  return items
}

function isBytesList(items: unknown[]): items is Buffer[] {
  for (const item of items) if (!Buffer.isBuffer(item)) return false
  return true
}

/** The two items of `items`, which holds no other. */
function twoOf<T>(items: readonly T[]): [T, T] {
  assert(items.length === 2, 'A Redis reply is not a pair')
  const first = items[0]
  const second = items[1]
  assert(first !== undefined && second !== undefined, 'A Redis reply is not a pair')
  return [first, second]
}

/** `visit` with each two items of `items` from `start` on. */
function eachPair(items: readonly unknown[], start: number, visit: (first: unknown, second: unknown) => void): void {
  assert((items.length - start) % 2 === 0, 'A Redis reply has an odd number of items')
  for (let i = start; i < items.length; i += 2) visit(items[i], items[i + 1])
}

/** `visit` with each three items of `items` from `start` on. */
function eachTriple(
  items: readonly unknown[],
  start: number,
  visit: (first: unknown, second: unknown, third: unknown) => void,
): void {
  assert((items.length - start) % 3 === 0, 'A Redis reply has a number of items that is not a multiple of three')
  for (let i = start; i < items.length; i += 3) visit(items[i], items[i + 1], items[i + 2])
}

/** A script's JSON reply, its shape still to be read. */
function json(reply: unknown): unknown {
  const value: unknown = JSON.parse(text(reply))
  return value
}
