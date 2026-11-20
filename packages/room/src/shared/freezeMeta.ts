export { freezeMeta }

import { isObject } from 'telefunc/__internal'
import { assertUsage } from '../utils/assert.js'

/** The methods that still change a frozen `Map`, `Set` or `Date`. */
const MAP_WRITES = ['set', 'delete', 'clear']
const SET_WRITES = ['add', 'delete', 'clear']
const DATE_WRITES = Object.getOwnPropertyNames(Date.prototype).filter((name) => name.startsWith('set'))

/** Makes a meta read-only all the way down, so a change is a new meta: its objects and arrays are frozen, the methods
 *  that change a `Map`, `Set` or `Date` in it throw, and each read of a typed array in it gets a copy of its own, as
 *  bytes can't be frozen. Only this function freezes a meta, or a package field's value, each Room's own copy
 *  (`ownMetadata()`, `ownFieldValue()`), so it stops at an object frozen already, and each object is walked once
 *  however many metas share it. */
function freezeMeta<Meta extends object>(meta: Meta): Meta {
  if (Object.isFrozen(meta) || ArrayBuffer.isView(meta)) return meta
  if (meta instanceof Map) {
    refuseWrites(meta, MAP_WRITES)
    for (const [key, value] of meta) {
      freezeEntry(key)
      freezeEntry(value)
    }
  } else if (meta instanceof Set) {
    refuseWrites(meta, SET_WRITES)
    for (const value of meta) freezeEntry(value)
  } else if (meta instanceof Date) {
    refuseWrites(meta, DATE_WRITES)
  } else {
    for (const [key, value] of Object.entries(meta)) {
      if (ArrayBuffer.isView(value))
        Object.defineProperty(meta, key, { get: () => structuredClone(value), enumerable: true })
      else if (isObject(value)) freezeMeta(value)
    }
  }
  return Object.freeze(meta)
}

/** A `Map`'s or a `Set`'s entry is read as it is kept, so it can't be a copy per read. */
function freezeEntry(value: unknown): void {
  assertUsage(
    !ArrayBuffer.isView(value),
    "Room can't keep a typed array in a meta's Map or Set read-only: put it in an object or an array of the meta",
  )
  if (isObject(value)) freezeMeta(value)
}

function refuseWrites(target: object, methods: string[]): void {
  for (const method of methods) Object.defineProperty(target, method, { value: refuseWrite })
}

function refuseWrite(): never {
  throw new TypeError('A Room meta is read-only: build a new value to change it')
}
