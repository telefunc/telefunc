export { StreamAbortedError }

/** A pushed body its producer ended at once (`abort()`), with what its consumer hadn't read: the consumer is let go of,
 *  a peer gone rather than a bug. */
class StreamAbortedError extends Error {
  constructor() {
    super('Stream aborted')
    this.name = 'StreamAbortedError'
  }
}
