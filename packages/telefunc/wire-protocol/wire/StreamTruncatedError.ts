export { StreamTruncatedError }

/** Who stopped the request body short: its client, which left (also where a read failed and nothing says why, which a
 *  Web runtime's body does as its client leaves); the body itself, which ended mid-frame; or the server, which answered
 *  or let it go before reading it to its end. */
type StoppedBy = 'client' | 'body' | 'server'

const STOPPED_BY: Record<StoppedBy, string> = {
  client: 'The client disconnected before the end of the request body',
  body: 'The request body ended before its end',
  server: 'The server answered before reading the request body to its end',
}

/** The request body stopped short. Typed so error sinks discriminate an expected truncation from a real server bug by
 *  class, not message text; the message says who stopped it. */
class StreamTruncatedError extends Error {
  readonly by: StoppedBy
  constructor(by: StoppedBy, options?: ErrorOptions) {
    super(STOPPED_BY[by], options)
    this.by = by
  }
}
