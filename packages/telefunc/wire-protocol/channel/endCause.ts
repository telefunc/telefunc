export { END_CAUSE, endCauseOf, refusalCauseOf }
export type { EndCause, FrameRefusalCause, RefusalCause, SessionCause }

import { assert } from '../../utils/assert.js'
import { NetworkError } from '../../shared/NetworkError.js'
import { CHANNEL_HOLD_MAX_BYTES } from '../constants.js'
import { ChannelClosedError, ChannelOverflowError, framesRefusedError } from './channelErrors.js'

/** Why a channel ended, as both ends of it say so: `code` is what an `ERROR` frame and a refusal carry on the wire,
 *  `error` what the channel ends with, given the page's `reconnectTimeout`. */
type EndCause = { readonly code: number; readonly error: (reconnectTimeoutMs: number) => Error }

/** Every cause a channel can end for that crosses from one end to the other, by name. Each code is one cause on both
 *  ends, so a page and a server agree on a channel's error from the code alone. */
const END_CAUSE = {
  /** Its page fell behind what the server holds for it, sent more than the server holds while its listeners work on it,
   *  or the server can't hold even a refusal of its ack requests. */
  overflow: {
    code: 0x01,
    error: () =>
      new ChannelOverflowError(
        `Channel closed: the server couldn't hold more for this client. What it had yet to receive, or what it sent that the server was still working on, passed what the server holds for one channel (${CHANNEL_HOLD_MAX_BYTES / 2 ** 20} MiB by default) or config.channel.maxMemory. Read messages faster, send less or await each send and publish(), publish with \`lossy: true\` where a client may skip to the newest, or raise config.channel.maxMemory`,
      ),
  },
  /** A reconnect needed frames its sender's replay buffer had dropped to stay within its size. */
  lost: {
    code: 0x02,
    error: () =>
      new NetworkError(
        "Channel closed: a reconnect needed messages the client's replay buffer had dropped to stay within its size. Await your sends.",
        true,
      ),
  },
  /** A broadcast's subscription didn't take effect: its backend refused it, or a call that carries it waited
   *  `connectTtl` for it, or its request went first. What the backend said stays on the server. */
  notInEffect: {
    code: 0x03,
    error: () =>
      new ChannelClosedError(
        'Broadcast closed: its subscription did not take effect on the server. Get the broadcast from a telefunction again to subscribe again.',
      ),
  },
  /** A broadcast's subscription that was in effect ended: its backend ended it for good. What the backend said stays on
   *  the server. */
  ended: {
    code: 0x0c,
    error: () =>
      new ChannelClosedError(
        'Broadcast closed: its subscription was in effect on the server until its backend ended it. Get the broadcast from a telefunction again to subscribe again.',
      ),
  },
  /** A graceful close's deadline passed before both ends were done: from the end that decides the close. */
  closeTimedOut: { code: 0x04, error: () => new ChannelClosedError('Channel close timed out') },
  /** Its page sent a frame of it over the server's frame limit. */
  frameOverLimit: frameCause(0x05, "A frame is over the server's frame limit."),
  /** Its page sent a frame of it the protocol doesn't allow. */
  frameRefused: frameCause(0x06, "The server can't take a frame of the page's."),
  /** Its page hasn't confirmed what the server sent on it for over `reconnectTimeout` of reconnects. */
  unconfirmed: {
    code: 0x07,
    error: (reconnectTimeoutMs: number) =>
      new NetworkError(
        `Channel closed: the page hasn't confirmed what the server sent on it for over ${reconnectTimeoutMs / 1_000} s of reconnects`,
        true,
      ),
  },
  /** Its page's connection sent what the server can't read as frames, or a frame of an index its session holds in no
   *  form: every channel of the session ends. */
  unreadable: sessionCause(0x08, ChannelClosedError, "The server can't read what the page sent on its connection."),
  /** Its page's connection sent more than the server holds while it works on what came before: every channel of the
   *  session ends. */
  receiveBudget: sessionCause(
    0x09,
    ChannelOverflowError,
    'The page sent more on its connection than the server holds while it works on what came before.',
  ),
  /** Its page's connection made the server hold more than its share of `maxMemory` beyond what its channels hold (the
   *  answers to what the page sent, the copies of what the server sent it), as its page didn't take it: every channel of
   *  the session ends. */
  sendBudget: sessionCause(
    0x0b,
    ChannelOverflowError,
    "The page didn't take what the server sent on its connection, and the server held more for it than its share of config.channel.maxMemory.",
  ),
  /** A reconnect needed its channel's end, which the server had no room to keep. */
  endNotKept: {
    code: 0x0a,
    error: () =>
      new NetworkError(
        "Channel closed: a reconnect needed the channel's end, which the server had no room to keep within config.channel.maxMemory.",
        true,
      ),
  },
} as const satisfies Record<string, EndCause>

/** The causes of a frame of one channel the server can't take, which end that channel. */
type FrameRefusalCause = 'frameOverLimit' | 'frameRefused'
/** The causes that end every channel of the page's session, the page's connection with them. */
type SessionCause = 'unreadable' | 'receiveBudget' | 'sendBudget'
/** The causes a `frames` refusal names. */
type RefusalCause = FrameRefusalCause | SessionCause
/** What a `frames` refusal's cause ends: the channel it names, or the page's session, naming none. */
type RefusalScope = 'channel' | 'session'

const causeOfCode: ReadonlyMap<number, (typeof END_CAUSE)[keyof typeof END_CAUSE]> = new Map(
  Object.values(END_CAUSE).map((cause) => [cause.code, cause]),
)
// Two causes on one code would end a channel with one cause's error on one end and the other's on the other.
assert(causeOfCode.size === Object.keys(END_CAUSE).length)

/** The cause `code` names; undefined for one this copy of Telefunc doesn't know. */
function endCauseOf(code: number): EndCause | undefined {
  return causeOfCode.get(code)
}

/** The cause of a `frames` refusal `code` names, with its scope; undefined for a code no refusal names. */
function refusalCauseOf(code: number) {
  const cause = causeOfCode.get(code)
  return cause !== undefined && 'scope' in cause ? cause : undefined
}

/** A frame the server can't take for `reason`, the sentence its refusal carries. */
function frameCause<Code extends number>(code: Code, reason: string) {
  const scope: RefusalScope = 'channel'
  return { code, reason, scope, error: () => framesRefusedError(reason) }
}

/** The page's connection ends for `reason`, the sentence its refusal carries: of the connection, so that it is true of
 *  every channel that ends with it, the ones that sent nothing wrong too. */
function sessionCause<Code extends number>(code: Code, errorClass: new (message: string) => Error, reason: string) {
  const scope: RefusalScope = 'session'
  return { code, reason, scope, error: () => new errorClass(`Channel closed: the page's connection ended: ${reason}`) }
}
