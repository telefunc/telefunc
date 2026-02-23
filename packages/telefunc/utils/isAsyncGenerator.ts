export function isAsyncGenerator(value: unknown): value is AsyncGenerator<unknown> {
  if (value == null || typeof value !== 'object') return false
  const obj = value as Record<string | symbol, unknown>
  return (
    typeof obj.next === 'function' &&
    typeof obj.return === 'function' &&
    typeof obj.throw === 'function' &&
    typeof obj[Symbol.asyncIterator] === 'function'
  )
}
