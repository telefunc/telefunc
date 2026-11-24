export { getGlobalObject }
export type { GlobalObjects }

/** The state each file keeps on `globalThis`, by the file's name, which must be unique: two module graphs of one process
 *  (a dev server's) share it. A file adds its entry to this interface (`declare module './getGlobalObject.js'`). */
interface GlobalObjects {}

declare global {
  var _telefunc_room: Partial<GlobalObjects> | undefined
}

/** File `key`'s state: `create()`'s on the first call in the process, the same object after. */
function getGlobalObject<Key extends keyof GlobalObjects>(
  key: Key,
  create: () => GlobalObjects[Key],
): GlobalObjects[Key] {
  const all = (globalThis._telefunc_room ??= {})
  return (all[key] ??= create())
}
