export { parseHttpRequest }

import type { Readable } from 'node:stream'
import { parseTransform, type Reviver } from '@brillout/json-serializer/parse'
import { assertUsage, getProjectError, assert } from '../../../utils/assert.js'
import { getTelefunctionKey } from '../../../utils/getTelefunctionKey.js'
import { getRequestPathname } from '../../../utils/getUrlPathname.js'
import { hasProp } from '../../../utils/hasProp.js'
import { isProduction } from '../../../utils/isProduction.js'
import { createRequestReviver, resolveDeferredRevivals } from '../../../wire-protocol/values/server/request/registry.js'
import { BodyReader, OversizeFrameError, StreamTruncatedError } from '../../../wire-protocol/wire/BodyReader.js'
import type { ClientLeft } from './clientLeft.js'
import type { TakesUpgrades } from './takesUpgrades.js'
import { REQUEST_KIND, REQUEST_KIND_HEADER, getRequestKind } from '../../../wire-protocol/wire/requestKind.js'
import type { RequestContext } from '../context/requestContext.js'
import { ServerChannel } from '../../../wire-protocol/channel/server/ServerChannel.js'
import { carryChannel } from './carryChannel.js'
import { ChannelStreamSource } from '../../../wire-protocol/values/ChannelStreamSource.js'
import { queueMemory } from '../../../wire-protocol/channel/server/channelMemory.js'
import { getGcRegistry } from '../../../utils/GcRegistry.js'
import { wrapProxy } from '../../../utils/wrapProxy.js'
import { isObjectOrFunction } from '../../../utils/isObjectOrFunction.js'
import type { ServerReviverContext } from '../../../wire-protocol/values/types.js'
import { CALL_METADATA_MAX_BYTES, isStreamTransport, type StreamTransport } from '../../../wire-protocol/constants.js'
import { isCallWant, type CallWant } from '../../../wire-protocol/wire/payloads.js'
import { handleSseChannelRequest, type SseChannelHttpResponse } from '../../../wire-protocol/channel/server/sse.js'
import { REQUEST_KIND_REFUSAL, type Refusal } from '../../../wire-protocol/refusal.js'
import { buildShieldValidators, getArgumentShields, type ShieldLogConfig } from '../shield.js'
import { toPathKey } from '../../../utils/pathKey.js'
import type { Telefunction } from '../types.js'
import { getServerExtensionTypes } from '../serverConfig.js'
import { handleTelefunctionBug } from './validateTelefunctionError.js'

type RunContext = {
  request: Request
  /** Node `IncomingMessage` (or any `Readable`) when the entry adapter has one. Body
   *  consumers (SSE upstream, binary RPC) prefer it over `request.body` to skip Node's
   *  webstreams `dequeueValue` overhead. Other runtimes only ever pass `request`. */
  readable?: Readable
  /** Whether a failed read of the body is the client leaving, as the adapter that received the request says. */
  clientLeft: ClientLeft
  /** Aborts once the client left, and only then, as the same adapter says. */
  clientGone: AbortSignal
  /** Whether the server that received the request takes WebSocket upgrades, as its adapter says. */
  takesUpgrades: TakesUpgrades
  requestContext: RequestContext
  logMalformedRequests: boolean
  serverConfig: {
    telefuncUrl: string
    stream: { transport: StreamTransport }
    log: { shieldErrors: ShieldLogConfig }
  }
}

type ResolvedRequest =
  | {
      isMalformedRequest: false
      telefunctionArgs: unknown[]
      streamTransport: StreamTransport
      requestExtensions: Record<string, Record<string, unknown>>
    }
  | { isMalformedRequest: true }

type ParseResult =
  | {
      telefuncFilePath: string
      telefunctionName: string
      telefunctionKey: string
      /** The wants of the page's broadcasts the server hasn't confirmed to it, where there are any. */
      wants: CallWant[] | undefined
      resolveRequest: (telefunction: Telefunction) => ResolvedRequest
      isSseRequest: false
      isMalformedRequest: false
    }
  | { isMalformedRequest: false; isSseRequest: true; sseResponse: SseChannelHttpResponse }
  /** `refusal`: Telefunc's own, which the answer marks. */
  | { isMalformedRequest: true; refusal?: Refusal }

async function parseHttpRequest(runContext: RunContext): Promise<ParseResult> {
  assertUrl(runContext)
  if (isWrongMethod(runContext)) return { isMalformedRequest: true }

  const { request, readable, clientLeft, takesUpgrades, requestContext, serverConfig } = runContext
  const requestKind = getRequestKind(request, serverConfig.telefuncUrl)

  if (requestKind === REQUEST_KIND.INVALID) {
    logParseError(
      `The \`${REQUEST_KIND_HEADER}\` header of the HTTP request is missing, invalid, or contradicts the \`_telefunc\` URL parameter.`,
      runContext,
    )
    return { isMalformedRequest: true, refusal: REQUEST_KIND_REFUSAL }
  }
  if (requestKind === REQUEST_KIND.SSE) {
    const sseResponse = await handleSseChannelRequest(request, readable, clientLeft, takesUpgrades)
    return { isMalformedRequest: false, isSseRequest: true, sseResponse }
  }

  // Body + base reviver context (file handling differs between binary and text; channels are the same).
  const createChannel = <ClientToServer, ServerToClient>(opts: { id: string; ack?: boolean }) => {
    const channel = new ServerChannel<ClientToServer, ServerToClient>(opts)
    carryChannel(channel, requestContext)
    return channel
  }
  const body = await readBody(request, readable, clientLeft, requestKind, runContext)
  if (body.isMalformedRequest) return body
  const { text, registerFile } = body
  const baseContext: Omit<ServerReviverContext, 'validators'> = {
    registerFile,
    createChannel,
    receiveStream: ({ channelId }) =>
      ChannelStreamSource.create(createChannel({ id: channelId }), { memory: queueMemory() }),
  }

  // Routing must be known before the target telefunc module can load. Parse JSON structure only;
  // leave serializer-encoded values untouched until that module has registered its wire types.
  const envelope = parseEnvelope(text, runContext)
  if (envelope.isMalformedRequest) return envelope

  const wants = wantsOf(envelope.raw)
  if (wants === null) {
    logParseError('Telefunc request body has unexpected `wants`', runContext)
    return { isMalformedRequest: true }
  }

  return {
    telefuncFilePath: envelope.telefuncFilePath,
    telefunctionName: envelope.telefunctionName,
    telefunctionKey: envelope.telefunctionKey,
    wants,
    isSseRequest: false,
    isMalformedRequest: false,
    resolveRequest(telefunction) {
      const { requestTypes } = getServerExtensionTypes()
      const { reviver, deferreds } = createRequestReviver(requestTypes)
      const resolvedEnvelope = transformEnvelope(envelope.raw, reviver, runContext)
      if (resolvedEnvelope.isMalformedRequest) return resolvedEnvelope
      // Shield metadata is attached by the generated code at module load. It's absent only when
      // the telefunction has no declared shields — in that case we revive without validators.
      const shields = getArgumentShields(telefunction) ?? {}
      const shieldCtx = {
        telefunctionName: resolvedEnvelope.telefunctionName,
        telefuncFilePath: resolvedEnvelope.telefuncFilePath,
        shieldErrors: runContext.serverConfig.log.shieldErrors,
      }
      resolveDeferredRevivals(
        resolvedEnvelope.args,
        deferreds,
        (segments) => ({
          ...baseContext,
          validators: buildShieldValidators(shields[toPathKey(segments)] ?? {}, shieldCtx),
        }),
        ({ value, close, abort, onEnd }) => {
          assert(isObjectOrFunction(value))
          // Holder side: the telefunction gets a GC-anchor wrapper; once it drops it, the
          // underlying channel/stream closes and the client releases the original.
          const wrapper = wrapProxy(value)
          // Holder-side GC tracking of revived request stubs (callback channels, streams). One shared
          // instance: it's a passive FinalizationRegistry, and its scan timer runs only while stubs are
          // tracked (see GcRegistry) — so it keeps nothing alive and doesn't block hibernation when idle.
          // It must NOT be created per-request: a per-request closure hung on the (rooted) request context
          // pins that request's `envelope.args` via the V8 scope chain, so the stub is never collected.
          const forget = getGcRegistry().register(wrapper, close)
          // Forgotten once nothing is left for `close` to do, so the scan doesn't wait for a collection to stop: at the
          // value's end where it has one (a File's, once its bytes are gone, which may be past the call), else once the
          // call's channels have closed.
          if (onEnd) onEnd(forget)
          else requestContext.onClose(forget)
          requestContext.onCallFailed(close)
          requestContext.responseAbort.onAbort(abort)
          return wrapper
        },
      )
      return {
        isMalformedRequest: false,
        telefunctionArgs: resolvedEnvelope.args,
        streamTransport: resolvedEnvelope.streamTransport,
        requestExtensions: resolvedEnvelope.requestExtensions,
      }
    },
  }
}

/** Reads the HTTP body and returns the metadata text plus the file handlers appropriate for the
 *  request kind. Binary requests frame files after the JSON envelope; text requests carry no files. A binary request
 *  whose metadata declares more than the server reads (`CALL_METADATA_MAX_BYTES`) is malformed, refused before any of its metadata is read,
 *  and so is one whose body ends before its envelope is complete. A
 *  binary request's body is read off to its end at the call's answer where a returned value holds the request
 *  (`keepRest`), else at its end (`readOffBody`), kept for the files a File can still read. */
async function readBody(
  request: Request,
  readable: Readable | undefined,
  clientLeft: ClientLeft,
  requestKind: typeof REQUEST_KIND.BINARY | typeof REQUEST_KIND.TEXT,
  runContext: RunContext,
): Promise<
  | {
      isMalformedRequest: false
      text: string
      registerFile: ServerReviverContext['registerFile']
    }
  | { isMalformedRequest: true }
> {
  if (requestKind === REQUEST_KIND.BINARY) {
    const source = readable ?? request.body
    if (!source) return refuseEndedEnvelope(runContext)
    const reader = new BodyReader(source, clientLeft, queueMemory)
    // A browser delivers the answer only once it has sent the whole body: where a returned value holds the request on,
    // the rest of the body is read now, and kept for the call's files within what the channels' memory allows.
    const clientGone = holdUploadResponse(runContext, reader.restRead)
    // A page attaches to a channel the call returns once its upload is over: until then, each byte of it says the page
    // is on its way. An upload that stops, at its end or as its client goes silent, leaves its last byte as the sign;
    // one that stopped short for a whole connect window is given up on, with its files.
    let lastByteAt = performance.now()
    reader.onChunk = () => {
      lastByteAt = performance.now()
    }
    runContext.requestContext.page = { lastSign: () => lastByteAt, neverCame: () => reader.clientGaveUp() }
    // From the answer on, a read of the body may not end where its client leaves (Node no longer watches the request):
    // the adapter's evidence says it. Watched again, it adds nothing.
    const leftByClient = () => reader.leftByClient(clientGone.reason)
    const watchClient = () => {
      clientGone.removeEventListener('abort', leftDuringCall)
      if (clientGone.aborted) leftByClient()
      else clientGone.addEventListener('abort', leftByClient, { once: true })
    }
    // While the call runs, the file reads end as the client leaves, once a read of the body under way took its failure.
    const leftDuringCall = () => void reader.readSettled().then(() => reader.clientGaveUp())
    clientGone.addEventListener('abort', leftDuringCall, { once: true })
    runContext.requestContext.onAnswered(() => {
      reader.keepRest()
      watchClient()
    })
    runContext.requestContext.onClose(() => void readOffBody(reader, clientGone, watchClient))
    const metadata = await reader
      .readFrame(CALL_METADATA_MAX_BYTES)
      .catch((err: unknown): 'oversize' | StreamTruncatedError => {
        if (err instanceof OversizeFrameError) return 'oversize'
        if (err instanceof StreamTruncatedError) return err
        throw err
      })
    if (metadata === 'oversize') {
      logParseError(
        `The binary request declares more metadata than the server reads (over ${CALL_METADATA_MAX_BYTES} bytes).`,
        runContext,
      )
      return { isMalformedRequest: true }
    }
    const envelope = metadata ?? reader.truncation()
    if (envelope instanceof StreamTruncatedError) {
      // A client that left, or a read nothing explains: the call's truncation.
      if (envelope.by !== 'body') throw envelope
      return refuseEndedEnvelope(runContext)
    }
    return {
      isMalformedRequest: false,
      text: new TextDecoder().decode(envelope),
      registerFile: (i, s) => reader.registerFile(i, s),
    }
  }
  return {
    isMalformedRequest: false,
    text: await readText(request, clientLeft),
    registerFile: () => ({
      stream: () =>
        new ReadableStream<Uint8Array<ArrayBuffer>>({
          start(controller) {
            controller.error(new Error('No binary frame'))
          },
        }),
      letGo() {},
      onGone: (callback) => callback(),
    }),
  }
}

/** Deno can stall reads when the response ends, and its legacy signal aborts at that point. Keep the response
 *  open until the upload ends or the client leaves. A string answer releases the hold itself. */
function holdUploadResponse({ request, clientGone, requestContext }: RunContext, restRead: Promise<void>): AbortSignal {
  // A Node adapter's socket signal stays valid after the response, including under Deno's node:http.
  if (!hasProp(globalThis, 'Deno', 'object') || clientGone !== request.signal) return clientGone
  const gone = new AbortController()
  const abort = () => gone.abort(clientGone.reason)
  if (clientGone.aborted) abort()
  else clientGone.addEventListener('abort', abort, { once: true })
  requestContext.responseHold = {
    over: Promise.race([restRead, aborted(gone.signal)]),
    unheld: () => clientGone.removeEventListener('abort', abort),
  }
  return gone.signal
}

/** A binary request whose body ends before its envelope is complete: no body (a Web `Request`'s `null`), an empty one, or
 *  one whose own end cuts its metadata. No Telefunc client sends one, so it is malformed. */
function refuseEndedEnvelope(runContext: RunContext): { isMalformedRequest: true } {
  logParseError('The binary request ends before its envelope is complete.', runContext)
  return { isMalformedRequest: true }
}

/** At the call's end, reads the rest of its body, kept for each File the app can still read and else dropped: a
 *  connection the client keeps alive carries its next request only after this one's body (Node's HTTP server parses it
 *  from there). On Node, the server's `requestTimeout` bounds the read; Bun and Deno let a client that trickles its body
 *  hold the connection as long as it sends, as they do without Telefunc. A failure that is the app's own error is a bug;
 *  a client that left ends it quietly. `watchClient`: from now on, the client's leaving ends the read. */
async function readOffBody(reader: BodyReader, signal: AbortSignal, watchClient: () => void): Promise<void> {
  // A client that leaves ends the call (`signal`) in the same event in which its body fails: the read of the body under
  // way settles first, so that the file reads end with that failure, or, with none under way, as the client's. A call
  // that answered has no failure coming.
  if (signal.aborted) await reader.readSettled()
  watchClient()
  try {
    await reader.endCall(signal.aborted ? 'client' : 'server')
  } catch (err: unknown) {
    handleTelefunctionBug(err)
  }
}

/** A text call's body, as the platform reads it. A read that fails as its client leaves is a truncated body, as a binary
 *  call's is; any other failure, and one nothing explains (`noTransportEvidence`), is thrown as it is. */
async function readText(request: Request, clientLeft: ClientLeft): Promise<string> {
  try {
    return await request.text()
  } catch (err) {
    if (clientLeft(err)) throw new StreamTruncatedError('client', { cause: err })
    throw err
  }
}

type Envelope =
  | {
      isMalformedRequest: false
      telefuncFilePath: string
      telefunctionName: string
      telefunctionKey: string
      raw: object
      args: unknown[]
      streamTransport: StreamTransport
      requestExtensions: Record<string, Record<string, unknown>>
    }
  | { isMalformedRequest: true }

/** Parse only enough of the request envelope to route it. Serializer transformation happens after
 *  the target module loads, using the extension types registered by that module. */
function parseEnvelope(text: string, runContext: RunContext): Envelope {
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch (err: unknown) {
    logParseException(err, runContext)
    return { isMalformedRequest: true }
  }

  return envelopeFrom(parsed, runContext)
}

function transformEnvelope(raw: object, reviver: Reviver, runContext: RunContext): Envelope {
  let parsed: unknown
  try {
    parsed = parseTransform(raw, { reviver })
  } catch (err: unknown) {
    logParseException(err, runContext)
    return { isMalformedRequest: true }
  }

  return envelopeFrom(parsed, runContext)
}

function envelopeFrom(parsed: unknown, runContext: RunContext): Envelope {
  if (!hasProp(parsed, 'file', 'string') || !hasProp(parsed, 'name', 'string') || !hasProp(parsed, 'args', 'array')) {
    logParseError('Telefunc request body has unexpected content', runContext)
    return { isMalformedRequest: true }
  }

  return {
    isMalformedRequest: false,
    telefuncFilePath: parsed.file,
    telefunctionName: parsed.name,
    telefunctionKey: getTelefunctionKey(parsed.file, parsed.name),
    raw: parsed,
    args: parsed.args,
    streamTransport: resolveStreamTransport(parsed, runContext.serverConfig.stream.transport),
    requestExtensions:
      hasProp(parsed, 'extensions', 'object') && parsed.extensions !== null
        ? (parsed.extensions as Record<string, Record<string, unknown>>)
        : {},
  }
}

/** The call's `wants`, of the page's broadcasts on all its connections: null where they're malformed. */
function wantsOf(raw: object): CallWant[] | undefined | null {
  if (!hasProp(raw, 'wants')) return undefined
  const { wants } = raw
  return Array.isArray(wants) && wants.every(isCallWant) ? wants : null
}

function logParseException(err: unknown, runContext: RunContext) {
  logParseError(
    ["Telefunc request body couldn't be parsed.", !hasProp(err, 'message') ? null : `Parse error: ${err.message}.`]
      .filter(Boolean)
      .join(' '),
    runContext,
  )
}

function resolveStreamTransport(parsed: object, fallback: StreamTransport): StreamTransport {
  if (!hasProp(parsed, 'stream', 'object') || !hasProp(parsed.stream, 'transport')) return fallback
  const { transport } = parsed.stream
  return isStreamTransport(transport) ? transport : fallback
}

function isWrongMethod(runContext: { request: Request; logMalformedRequests: boolean }) {
  const { method } = runContext.request
  if (method === 'POST' || method === 'post') return false
  assert(typeof method === 'string')
  logParseError(`The HTTP request method should be \`POST\` (or \`post\`) but \`method === '${method}'\`.`, runContext)
  return true
}

function assertUrl(runContext: { request: Request; serverConfig: { telefuncUrl: string } }) {
  const urlPathname = getRequestPathname(runContext.request.url)
  assertUsage(
    urlPathname === runContext.serverConfig.telefuncUrl,
    `serve({ url }): The pathname of \`url\` is \`${urlPathname}\` but it's expected to be \`${runContext.serverConfig.telefuncUrl}\`. Either make sure that \`url\` is the HTTP request URL, or set \`config.telefuncUrl\` to \`${urlPathname}\`.`,
  )
}

function logParseError(errMsg: string, runContext: { logMalformedRequests: boolean }) {
  if (!runContext.logMalformedRequests) return
  if (!isProduction()) {
    errMsg = `Malformed request in development. ${errMsg} This is unexpected since, in development, all requests are expected to originate from the Telefunc Client and should therefore be properly structured.`
  }
  console.error(getProjectError(errMsg))
}

/** Settles once `signal` aborts. */
function aborted(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve()
  return new Promise((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }))
}
