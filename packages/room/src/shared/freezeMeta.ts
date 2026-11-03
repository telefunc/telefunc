export { freezeMeta }

/** Freezes a meta and every object in it, so a change is a new meta: a reader holding one, such as a queued message,
 *  never sees it change. Only this function freezes a meta, so it stops at an object frozen already, and each object
 *  is walked once however many metas share it. */
function freezeMeta<Meta extends object>(meta: Meta): Meta {
  if (Object.isFrozen(meta)) return meta
  Object.freeze(meta)
  for (const value of Object.values(meta)) if (typeof value === 'object' && value !== null) freezeMeta(value)
  return meta
}
