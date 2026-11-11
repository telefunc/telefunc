export { freezeMeta }

/** Freezes a meta and the plain objects and arrays in it, so a change is a new meta. A `Map`, `Set` or `Date` in it is
 *  frozen too, but its methods still change it, and a `Uint8Array` isn't frozen, as `Object.freeze()` refuses bytes:
 *  that a reader holding the meta, such as a queued message, never sees one change rests on meta being read-only. Only
 *  this function freezes a meta, so it stops at an object frozen already, and each object is walked once however many
 *  metas share it. */
function freezeMeta<Meta extends object>(meta: Meta): Meta {
  if (Object.isFrozen(meta) || ArrayBuffer.isView(meta)) return meta
  Object.freeze(meta)
  for (const value of Object.values(meta)) if (typeof value === 'object' && value !== null) freezeMeta(value)
  return meta
}
