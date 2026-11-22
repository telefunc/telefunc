export { heartbeatHold, keptEndWindowOf, pingDeadline, reconnectWindowOf, PING_INTERVAL_MAX_MS }

import {
  CHANNEL_PING_INTERVAL_MS,
  CHANNEL_RECONNECT_TIMEOUT_MS,
  CHANNEL_TRANSPORT,
  RECONCILE_TIMEOUT_MS,
  TIMER_DELAY_MAX_MS,
  type ChannelTransport,
} from '../constants.js'

/** How long the page may hold its PING so that it rides a request: over SSE, where its frames go in POSTs, half an
 *  interval; over a WebSocket, none. */
function heartbeatHold(intervalMs: number, transport: ChannelTransport): number {
  return transport === CHANNEL_TRANSPORT.SSE ? Math.floor(intervalMs / 2) : 0
}

/** How long a wire may deliver nothing before it is taken for dead: two ping intervals, after the time the page may
 *  hold its PING, which delays the PING the server waits for and the PONG that answers it alike. */
function pingDeadline(intervalMs: number, transport: ChannelTransport): number {
  return intervalMs * 2 + heartbeatHold(intervalMs, transport)
}

/** The longest interval whose deadline a timer can wait: over SSE, where it is longest, two and a half intervals. */
const PING_INTERVAL_MAX_MS = Math.floor((TIMER_DELAY_MAX_MS * 2) / 5)

/** How long one end holds the other that went away: until the ping deadline notices, then `reconnectTimeoutMs` more. */
function reconnectWindowOf(pingIntervalMs: number, reconnectTimeoutMs: number, transport: ChannelTransport): number {
  return Math.min(TIMER_DELAY_MAX_MS, pingDeadline(pingIntervalMs, transport) + reconnectTimeoutMs)
}

/** How long the server keeps what it knows of a session that left it, from the time it decided: its page notices the
 *  loss within a ping deadline, tries for a ping deadline and `reconnectTimeoutMs` from then, and its last attempt's
 *  RECONCILE has `RECONCILE_TIMEOUT_MS` to arrive. Each knob is the larger of the server's and its default, which a page
 *  that never read a RECONCILED runs on; the ping deadline is SSE's, the longer. Not clamped: it is added to an instant,
 *  which `DeadlineTimer` waits for however far off. */
function keptEndWindowOf(pingIntervalMs: number, reconnectTimeoutMs: number): number {
  const deadline = pingDeadline(Math.max(pingIntervalMs, CHANNEL_PING_INTERVAL_MS), CHANNEL_TRANSPORT.SSE)
  return 2 * deadline + Math.max(reconnectTimeoutMs, CHANNEL_RECONNECT_TIMEOUT_MS) + RECONCILE_TIMEOUT_MS
}
