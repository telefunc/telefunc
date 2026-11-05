export { getOrCreate }

function getOrCreate<Key, Value>(map: Map<Key, Value>, key: Key, create: () => Value): Value {
  const existing = map.get(key)
  if (existing !== undefined) return existing
  const value = create()
  map.set(key, value)
  return value
}
