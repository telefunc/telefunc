export { PendingValue }

/** Revived value known only once `promise` resolves: the call waits for it and hands out the resolved value in its slot. */
class PendingValue<T> {
  constructor(readonly promise: Promise<T>) {}
}
