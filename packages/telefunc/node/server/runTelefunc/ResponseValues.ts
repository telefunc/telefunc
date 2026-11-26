export { ResponseValues }
export type { ResponseValuesRun, SerializeResult, TelefuncId }

import { assertUsage } from '../../../utils/assert.js'
import { isObjectOrFunction } from '../../../utils/isObjectOrFunction.js'
import { ServerChannel, type ResponseChannel } from '../../../wire-protocol/channel/server/ServerChannel.js'
import { ServerBroadcastEnd } from '../../../wire-protocol/channel/server/ServerBroadcastEnd.js'
import { getSessionToken } from '../../../wire-protocol/wire/requestKind.js'
import { getAdapterMaxFrameBytes } from '../serverConfig.js'
import { buildShieldValidators, type ShieldValidators, type ValueShields, type ShieldLogConfig } from '../shield.js'
import { buildInlineResponseBody } from '../../../wire-protocol/values/server/response/StreamingResponseBody.js'
import { pumpProducerToChannel } from '../../../wire-protocol/values/server/response/ChannelResponseBody.js'
import { STREAM_TRANSPORT, type StreamTransport } from '../../../wire-protocol/constants.js'
import { textEncoder } from '../../../wire-protocol/wire/envelope.js'
import { uint8ArrayToBase64url } from '../../../wire-protocol/wire/base64url.js'
import type {
  ServerReplacerContext,
  StreamingProducer,
  StreamingValueServer,
} from '../../../wire-protocol/values/types.js'
import type { RequestContext } from '../context/requestContext.js'
import { carryChannel } from './carryChannel.js'
import type { AbortError } from '../Abort.js'
import { restoreContext, type Context } from '../context/context.js'
import type { Readable } from 'node:stream'

type TelefuncId = {
  telefunctionName: string
  telefuncFilePath: string
}

type SerializeResult =
  | { type: 'text'; body: string }
  | {
      type: 'streaming'
      body: Readable | ReadableStream<Uint8Array<ArrayBuffer>>
      streamTransport: StreamTransport
    }

type ResponseValuesRun = TelefuncId & {
  context: Context
  requestContext: RequestContext
  streamTransport: StreamTransport
  useNodeStream: boolean
  serverConfig: { log: { shieldErrors: ShieldLogConfig } }
  valueShields?: ValueShields
}

/** A channel the response carries, or one a returned stream is pumped through. */
type Staged =
  | { channel: ResponseChannel; producer?: undefined }
  | { channel: ServerChannel<never, never>; producer: StreamingProducer }

/** What one response carries beside its text: the channels and streams its values were replaced with. They're staged
 *  as the response is serialized and started by `finish` once it is sure to go; a response that fails, or whose page
 *  left, ends what only it would carry, and a channel an earlier response carried stays with its page. */
class ResponseValues {
  private readonly run: ResponseValuesRun
  private staged: Staged[] | null = null
  private inline: StreamingValueServer[] | null = null
  private started = false
  /** The response holds a channel an earlier response carried. */
  private carriesAnEarlierChannel = false
  /** The response's own store, shared by every replacer in it (`ServerReplacerContext.responseState`). */
  private readonly states = new Map<symbol, unknown>()
  /** Read once a replacer asks, so a response that returns no value of a page parses no URL. */
  private page: string | undefined

  constructor(run: ResponseValuesRun) {
    this.run = run
  }

  /** The context the replacer of `value` stages what it carries through. */
  contextFor(value: unknown): ServerReplacerContext {
    const values = this
    return {
      createChannel: this.createChannel,
      registerChannel: this.registerChannel,
      createBroadcastChannel: this.createBroadcastChannel,
      sendStream: this.sendStream,
      responseState: this.responseState,
      get page() {
        return (values.page ??= getSessionToken(values.run.requestContext.request) ?? crypto.randomUUID())
      },
      validators: this.validatorsOf(value),
      maxFrameBytes: getAdapterMaxFrameBytes(),
    }
  }

  /** A replaced value's `abort` ends it with the response, but for a channel an earlier response carried: this
   *  response fails, and the channel stays with its page. */
  onReplaced({ value, abort }: { value: unknown; abort: (abortError: AbortError) => void }): void {
    if (ServerChannel.isServerChannel(value) && value._wasRegistered) this.carriesAnEarlierChannel = true
    else this.run.requestContext.responseAbort.onAbort(abort)
  }

  /** The response, serialized as `text`: what it carries starts, unless it can't go. */
  finish(text: string): SerializeResult {
    const { run } = this
    const { requestContext } = run
    if (this.carriesAnEarlierChannel) {
      requestContext.responseAbort.abort()
      assertUsage(
        false,
        [
          `The value returned by telefunction ${run.telefunctionName}() (${run.telefuncFilePath}) holds a channel that an earlier telefunction call returned.`,
          'A channel connects the server to one client: return a `new Channel()` from each call, and for many clients a `new BroadcastChannel({ key })` from each call, with the same `key`.',
        ].join(' '),
      )
    }
    // Its page left while the telefunction ran: nobody gets this response.
    if (requestContext.abortSignal.aborted) {
      requestContext.responseAbort.abort()
      return { type: 'text', body: text }
    }
    this.started = true
    for (const staged of this.staged ?? []) this.start(staged)
    if (this.inline === null) return { type: 'text', body: text }
    return {
      type: 'streaming',
      body: buildInlineResponseBody({
        metadataSerialized: text,
        producers: this.inline,
        telefuncId: { telefunctionName: run.telefunctionName, telefuncFilePath: run.telefuncFilePath },
        context: run.context,
        abortSignal: requestContext.abortSignal,
        responseAbort: requestContext.responseAbort,
        onComplete: requestContext.trackPending(),
        encodeFrame:
          run.streamTransport === STREAM_TRANSPORT.SSE_INLINE
            ? (frame) => textEncoder.encode(`data: ${uint8ArrayToBase64url(frame)}\n\n`)
            : (frame) => frame,
        useNodeStream: run.useNodeStream,
      }),
      streamTransport: run.streamTransport,
    }
  }

  private start({ channel, producer }: Staged): void {
    const { requestContext } = this.run
    if (producer) return pumpProducerToChannel(channel, producer, this.run, requestContext.trackPending())
    channel._validators = this.validatorsOf(channel)
    carryChannel(channel, requestContext)
  }

  private stage(staged: Staged): void {
    this.staged ??= []
    this.staged.push(staged)
  }

  private readonly createChannel = <ClientToServer, ServerToClient>(opts?: { ack?: boolean }) => {
    const channel = new ServerChannel<ClientToServer, ServerToClient>(opts)
    this.stage({ channel })
    return channel
  }

  private readonly registerChannel = (channel: ResponseChannel): void => {
    this.stage({ channel })
  }

  private readonly createBroadcastChannel = (): ServerBroadcastEnd => {
    const end = new ServerBroadcastEnd()
    this.stage({ channel: end })
    return end
  }

  private readonly responseState = <T>(key: symbol, init: () => T): T => {
    if (!this.states.has(key)) this.states.set(key, init())
    return this.states.get(key) as T
  }

  private readonly sendStream = (createProducer: () => StreamingProducer) => {
    // Taken as it's serialized: a stream that can't be read fails the serialization, which ends what the response carries.
    const producer = producerInContext(createProducer(), this.run.context)
    const stream = {
      close() {},
      abort: (abortError: AbortError) => {
        if (!this.started) producer.cancel(abortError)
      },
    }
    if (this.run.streamTransport === STREAM_TRANSPORT.CHANNEL) {
      const channel = new ServerChannel<never, never>()
      this.stage({ channel, producer })
      return { metadata: { channelId: channel.id }, ...stream }
    }
    this.inline ??= []
    const index = this.inline.length
    this.inline.push({ producer, index })
    return { metadata: { __index: index }, ...stream }
  }

  /** The shields of `value`, as validators that log what they refuse; none where it has none. */
  private validatorsOf(value: unknown): ShieldValidators {
    const { valueShields } = this.run
    const shields = valueShields && isObjectOrFunction(value) ? valueShields.get(value) : undefined
    if (!shields) return new Map()
    const { telefunctionName, telefuncFilePath, serverConfig } = this.run
    return buildShieldValidators(shields, {
      telefunctionName,
      telefuncFilePath,
      shieldErrors: serverConfig.log.shieldErrors,
    })
  }
}

/** The pumps pull and cancel a generator or stream the telefunction returned with its request's context. */
function producerInContext(producer: StreamingProducer, context: Context): StreamingProducer {
  let chunks: StreamingProducer['chunks'] | undefined
  return {
    chunks: { next: () => restoreContext(context, () => (chunks ??= producer.chunks).next()) },
    cancel: (reason) => restoreContext(context, () => producer.cancel(reason)),
  }
}
