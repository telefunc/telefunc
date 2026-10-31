export { isPromise }

function isPromise(val: unknown): val is Promise<unknown> {
  return typeof val === 'object' && val !== null && 'then' in val && typeof val.then === 'function'
}
