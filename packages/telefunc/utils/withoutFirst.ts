export { withoutFirst }

/** A copy of `list` without its first `item`, or `list` itself without one: listeners are replaced, never mutated, so a
 *  dispatch iterates the ones it started with. */
function withoutFirst<T>(list: T[], item: T): T[] {
  const i = list.indexOf(item)
  return i < 0 ? list : list.filter((_, j) => j !== i)
}
