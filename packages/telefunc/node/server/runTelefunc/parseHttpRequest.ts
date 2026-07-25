export { parseHttpRequest }

import type { Readable } from 'node:stream'
import { parseTransform, type Reviver } from '@brillout/json-serializer/parse'
import { assertUsage, getProjectError, assert } from '../../../utils/assert.js'
import { getTelefunctionKey } from '../../../utils/getTelefunctionKey.js'
import { getUrlPathname } from '../../../utils/getUrlPathname.js'
import { hasProp } from '../../../utils/hasProp.js'
import { isProduction } from '../../../utils/isProduction.js'
import { createRequestReviver, resolveDeferredRevivals } from '../../../wire-protocol/server/request/registry.js'
import { StreamReader } from '../../../wire-protocol/server/request/StreamReader.js'
import { REQUEST_KIND, getRequestKind } from '../../../wire-protocol/request-kind.js'
import type { RequestContext } from '../context/requestContext.js'
import { ServerChannel } from '../../../wire-protocol/server/channel.js'
import { getChannelMux } from '../../../wire-protocol/server/mux.js'
import { ChannelStreamSource } from '../../../wire-protocol/ChannelStreamSource.js'
import { GcRegistry } from '../../../wire-protocol/gcRegistry.js'
import { wrapProxy } from '../../../wire-protocol/wrapProxy.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { isObjectOrFunction } from '../../../utils/isObjectOrFunction.js'
import type { ServerReviverContext } from '../../../wire-protocol/types.js'
import { STREAM_TRANSPORT, type StreamTransport } from '../../../wire-protocol/constants.js'
import { handleSseChannelRequest, type SseChannelHttpResponse } from '../../../wire-protocol/server/sse.js'
import { buildShieldValidators, getArgumentShields, type ShieldLogConfig } from '../shield.js'
import { toPathKey } from '../../../utils/pathKey.js'
import type { Telefunction } from '../types.js'
import { getServerExtensionTypes } from '../serverConfig.js'

// Holder-side GC tracking of revived request stubs (callback channels, streams). One shared
// instance: it's a passive FinalizationRegistry, and its scan timer runs only while stubs are
// tracked (see GcRegistry) — so it keeps nothing alive and doesn't block hibernation when idle.
// It must NOT be created per-request: a per-request closure hung on the (rooted) request context
// pins that request's `envelope.args` via the V8 scope chain, so the stub is never collected.
const globalObject = getGlobalObject('node/server/runTelefunc/parseHttpRequest.ts', {
  gcRegistry: new GcRegistry(),
})

type RunContext = {
  request: Request
  /** Node `IncomingMessage` (or any `Readable`) when the entry adapter has one. Body
   *  consumers (SSE upstream, binary RPC) prefer it over `request.body` to skip Node's
   *  webstreams `dequeueValue` overhead. Other runtimes only ever pass `request`. */
  readable?: Readable
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
      resolveRequest: (telefunction: Telefunction) => ResolvedRequest
      isSseRequest: false
      isMalformedRequest: false
    }
  | { isMalformedRequest: false; isSseRequest: true; sseResponse: SseChannelHttpResponse }
  | { isMalformedRequest: true }

async function parseHttpRequest(runContext: RunContext): Promise<ParseResult> {
  assertUrl(runContext)
  if (isWrongMethod(runContext)) return { isMalformedRequest: true }

  const { request, readable, requestContext, serverConfig } = runContext
  const requestKind = getRequestKind(request, serverConfig.telefuncUrl)

  if (requestKind === REQUEST_KIND.MISMATCH) return { isMalformedRequest: true }
  if (requestKind === REQUEST_KIND.SSE) {
    const sseResponse = await handleSseChannelRequest(request, readable)
    return sseResponse ? { isMalformedRequest: false, isSseRequest: true, sseResponse } : { isMalformedRequest: true }
  }

  // Body + base reviver context (file handling differs between binary and text; channels are the same).
  const mux = getChannelMux()
  const createChannel = <ClientToServer, ServerToClient>(opts: { id: string; ack?: boolean }) => {
    const channel = new ServerChannel<ClientToServer, ServerToClient>(opts)
    mux.registerChannel(channel)
    channel._setResponseAbort(requestContext.responseAbort.abort)
    channel.onClose(requestContext.trackPending())
    return channel
  }
  const { text, registerFile, consumeFile } = await readBody(request, readable, requestKind)
  const baseContext: Omit<ServerReviverContext, 'validators'> = {
    registerFile,
    consumeFile,
    createChannel,
    receiveStream: ({ channelId }) => ChannelStreamSource.create(createChannel({ id: channelId })),
  }

  // Routing must be known before the target telefunc module can load. Parse JSON structure only;
  // leave serializer-encoded values untouched until that module has registered its wire types.
  const envelope = parseEnvelope(text, runContext)
  if (envelope.isMalformedRequest) return envelope

  return {
    telefuncFilePath: envelope.telefuncFilePath,
    telefunctionName: envelope.telefunctionName,
    telefunctionKey: envelope.telefunctionKey,
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
        (revived) => {
          {
            // Destructure into locals so nothing here closes over `revived` — a closure over it
            // would pin `revived.value` (the wrapper) and defeat the GC-close. Same discipline as
            // the client response path.
            const { value, close } = revived
            assert(isObjectOrFunction(value))
            // Holder side: the telefunction gets a GC-anchor wrapper; once it drops it, the
            // underlying channel/stream closes and the client releases the original.
            const wrapper = wrapProxy(value)
            globalObject.gcRegistry.register(wrapper, close)
            revived.value = wrapper
          }
          {
            const { close, abort } = revived
            requestContext.onTopLevelError(close)
            requestContext.responseAbort.onAbort(abort)
          }
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
 *  request kind. Binary requests frame files after the JSON envelope; text requests carry no files. */
async function readBody(
  request: Request,
  readable: Readable | undefined,
  requestKind: typeof REQUEST_KIND.BINARY | typeof REQUEST_KIND.TEXT | null,
): Promise<{
  text: string
  registerFile: ServerReviverContext['registerFile']
  consumeFile: ServerReviverContext['consumeFile']
}> {
  if (requestKind === REQUEST_KIND.BINARY) {
    const source = readable ?? request.body
    assert(source)
    const reader = new StreamReader(source)
    return {
      text: await reader.readMetadata(),
      registerFile: (i, s) => reader.registerFile(i, s),
      consumeFile: (i, s) => reader.consumeFile(i, s),
    }
  }
  return {
    text: await request.text(),
    registerFile: () => {},
    consumeFile: () =>
      new ReadableStream<Uint8Array<ArrayBuffer>>({
        start(controller) {
          controller.error(new Error('No binary frame'))
        },
      }),
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

function logParseException(err: unknown, runContext: RunContext) {
  logParseError(
    ["Telefunc request body couldn't be parsed.", !hasProp(err, 'message') ? null : `Parse error: ${err.message}.`]
      .filter(Boolean)
      .join(' '),
    runContext,
  )
}

const VALID_STREAM_TRANSPORTS: StreamTransport[] = [
  STREAM_TRANSPORT.BINARY_INLINE,
  STREAM_TRANSPORT.SSE_INLINE,
  STREAM_TRANSPORT.CHANNEL,
]

function resolveStreamTransport(parsed: object, fallback: StreamTransport): StreamTransport {
  if (!hasProp(parsed, 'stream', 'object') || !hasProp(parsed.stream, 'transport', 'string')) return fallback
  const transport = parsed.stream.transport as StreamTransport
  return VALID_STREAM_TRANSPORTS.includes(transport) ? transport : fallback
}

function isWrongMethod(runContext: { request: Request; logMalformedRequests: boolean }) {
  const { method } = runContext.request
  if (method === 'POST' || method === 'post') return false
  assert(typeof method === 'string')
  logParseError(`The HTTP request method should be \`POST\` (or \`post\`) but \`method === '${method}'\`.`, runContext)
  return true
}

function assertUrl(runContext: { request: Request; serverConfig: { telefuncUrl: string } }) {
  const urlPathname = getUrlPathname(runContext.request.url)
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
