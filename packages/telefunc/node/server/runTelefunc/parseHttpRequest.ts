export { parseHttpRequest }

import { parse, type Reviver } from '@brillout/json-serializer/parse'
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
import { ChannelChunkReader } from '../../../wire-protocol/ChannelChunkReader.js'
import type { ReviverType, TypeContract, ServerReviverContext } from '../../../wire-protocol/types.js'
import { STREAM_TRANSPORT, type StreamTransport } from '../../../wire-protocol/constants.js'
import { handleSseChannelRequest, type SseChannelHttpResponse } from '../../../wire-protocol/server/sse.js'
import { buildShieldValidators, getArgumentShields, type ShieldLogConfig } from '../shield.js'
import { toPathKey } from '../../../utils/pathKey.js'
import type { Telefunction } from '../types.js'

type RunContext = {
  request: Request
  requestContext: RequestContext
  logMalformedRequests: boolean
  serverConfig: {
    telefuncUrl: string
    stream: { transport: StreamTransport }
    extensionRequestTypes: ReviverType<TypeContract, ServerReviverContext>[]
    log: { shieldErrors: ShieldLogConfig }
  }
}

type ParseResult =
  | {
      telefuncFilePath: string
      telefunctionName: string
      telefunctionKey: string
      reviveArgs: (telefunction: Telefunction) => unknown[]
      streamTransport: StreamTransport
      requestExtensions: Record<string, Record<string, unknown>>
      isSseRequest: false
      isMalformedRequest: false
    }
  | { isMalformedRequest: false; isSseRequest: true; sseResponse: SseChannelHttpResponse }
  | { isMalformedRequest: true }

async function parseHttpRequest(runContext: RunContext): Promise<ParseResult> {
  assertUrl(runContext)
  if (isWrongMethod(runContext)) return { isMalformedRequest: true }

  const { request, requestContext, serverConfig } = runContext
  const requestKind = getRequestKind(request, serverConfig.telefuncUrl)

  if (requestKind === REQUEST_KIND.MISMATCH) return { isMalformedRequest: true }
  if (requestKind === REQUEST_KIND.SSE) {
    const sseResponse = await handleSseChannelRequest(request)
    return sseResponse
      ? { isMalformedRequest: false, isSseRequest: true, sseResponse }
      : { isMalformedRequest: true }
  }

  // Body + base reviver context (file handling differs between binary and text; channels are the same).
  const createChannel = <TOut, TIn>(opts: { id: string; ack?: boolean }) => {
    const channel = new ServerChannel<TOut, TIn>(opts)
    channel._registerChannel()
    channel._setResponseAbort(requestContext.responseAbort.abort)
    channel.onClose(requestContext.trackPending())
    return channel
  }
  const { text, registerFile, consumeFile } = await readBody(request, requestKind)
  const baseContext: Omit<ServerReviverContext, 'validators'> = {
    registerFile,
    consumeFile,
    createChannel,
    receiveStreamReader: ({ channelId }) => ChannelChunkReader.create(createChannel({ id: channelId })),
    receiveStream: ({ channelId }) => ChannelChunkReader.toReadableStream(createChannel({ id: channelId })),
  }

  // Parse the envelope; each reviver-prefixed string becomes a deferred placeholder that
  // `reviveArgs(telefunction)` will resolve post-findTelefunction with shield-aware validators.
  const { reviver, deferreds } = createRequestReviver(serverConfig.extensionRequestTypes)
  const envelope = parseEnvelope(text, reviver, runContext)
  if (envelope.isMalformedRequest) return envelope

  return {
    ...envelope,
    isSseRequest: false,
    isMalformedRequest: false,
    reviveArgs(telefunction) {
      // Shield metadata is attached by the generated code at module load. It's absent only when
      // the telefunction has no declared shields — in that case we revive without validators.
      const shields = getArgumentShields(telefunction) ?? {}
      const shieldCtx = {
        telefunctionName: envelope.telefunctionName,
        telefuncFilePath: envelope.telefuncFilePath,
        shieldErrors: runContext.serverConfig.log.shieldErrors,
      }
      resolveDeferredRevivals(
        envelope.args,
        deferreds,
        (segments) => ({
          ...baseContext,
          validators: buildShieldValidators(shields[toPathKey(segments)] ?? {}, shieldCtx),
        }),
        ({ close, abort }) => {
          requestContext.onTopLevelError(close)
          requestContext.responseAbort.onAbort(abort)
        },
      )
      return envelope.args
    },
  }
}

/** Reads the HTTP body and returns the metadata text plus the file handlers appropriate for the
 *  request kind. Binary requests frame files after the JSON envelope; text requests carry no files. */
async function readBody(
  request: Request,
  requestKind: typeof REQUEST_KIND.BINARY | typeof REQUEST_KIND.TEXT | null,
): Promise<{ text: string; registerFile: ServerReviverContext['registerFile']; consumeFile: ServerReviverContext['consumeFile'] }> {
  if (requestKind === REQUEST_KIND.BINARY) {
    assert(request.body)
    const reader = new StreamReader(request.body)
    return {
      text: await reader.readMetadata(),
      registerFile: (i, s) => reader.registerFile(i, s),
      consumeFile: (i, s) => reader.consumeFile(i, s),
    }
  }
  return {
    text: await request.text(),
    registerFile: () => {},
    consumeFile: () => Promise.reject(new Error('No binary frame')),
  }
}

type Envelope =
  | {
      isMalformedRequest: false
      telefuncFilePath: string
      telefunctionName: string
      telefunctionKey: string
      args: unknown[]
      streamTransport: StreamTransport
      requestExtensions: Record<string, Record<string, unknown>>
    }
  | { isMalformedRequest: true }

/** Parse the request envelope and validate its shape. Each reviver-prefixed string in `args` becomes
 *  a deferred placeholder (see `createRequestReviver`) — actual value construction is deferred until
 *  `reviveArgs(telefunction)` runs in the caller, when shield metadata is available. */
function parseEnvelope(text: string, reviver: Reviver, runContext: RunContext): Envelope {
  let parsed: unknown
  try {
    parsed = parse(text, { reviver })
  } catch (err: unknown) {
    logParseError(
      [
        "Telefunc request body couldn't be parsed.",
        !hasProp(err, 'message') ? null : `Parse error: ${err.message}.`,
      ]
        .filter(Boolean)
        .join(' '),
      runContext,
    )
    return { isMalformedRequest: true }
  }

  if (!hasProp(parsed, 'file', 'string') || !hasProp(parsed, 'name', 'string') || !hasProp(parsed, 'args', 'array')) {
    logParseError('Telefunc request body has unexpected content', runContext)
    return { isMalformedRequest: true }
  }

  return {
    isMalformedRequest: false,
    telefuncFilePath: parsed.file,
    telefunctionName: parsed.name,
    telefunctionKey: getTelefunctionKey(parsed.file, parsed.name),
    args: parsed.args,
    streamTransport: resolveStreamTransport(parsed, runContext.serverConfig.stream.transport),
    requestExtensions:
      hasProp(parsed, 'extensions', 'object') && parsed.extensions !== null
        ? (parsed.extensions as Record<string, Record<string, unknown>>)
        : {},
  }
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
    `telefunc({ url }): The pathname of \`url\` is \`${urlPathname}\` but it's expected to be \`${runContext.serverConfig.telefuncUrl}\`. Either make sure that \`url\` is the HTTP request URL, or set \`config.telefuncUrl\` to \`${urlPathname}\`.`,
  )
}

function logParseError(errMsg: string, runContext: { logMalformedRequests: boolean }) {
  if (!runContext.logMalformedRequests) return
  if (!isProduction()) {
    errMsg = `Malformed request in development. ${errMsg} This is unexpected since, in development, all requests are expected to originate from the Telefunc Client and should therefore be properly structured.`
  }
  console.error(getProjectError(errMsg))
}
