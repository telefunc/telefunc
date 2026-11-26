export { serializeTelefunctionArguments, serializeHeldArguments, cancelUnsentArguments }
export type { HeldArguments }

import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { assert, assertUsage } from '../../utils/assert.js'
import { hasProp } from '../../utils/hasProp.js'
import { lowercaseFirstLetter } from '../../utils/lowercaseFirstLetter.js'
import { createRequestReplacer } from '../../wire-protocol/values/client/request/registry.js'
import { encodeRequestEnvelope } from '../../wire-protocol/wire/envelope.js'
import { loadedFeatures, loadedFeature } from '../../wire-protocol/values/client/loadFeatures.js'
import { SERIALIZER_PREFIX_FILE, SERIALIZER_PREFIX_BLOB } from '../../wire-protocol/constants.js'
import { randomUuid } from '../../utils/randomUuid.js'
import { getConnectionOptions } from './getConnectionOptions.js'
import { isObjectOrFunction } from '../../utils/isObjectOrFunction.js'
import { makeAbortError } from './errors.js'
import type { ChannelTransports, StreamTransport } from '../../wire-protocol/constants.js'
import type { ReplacerType, TypeContract, ClientReplacerContext } from '../../wire-protocol/values/types.js'
import { CloseHandler } from '../close.js'
import { getGcRegistry } from '../../utils/GcRegistry.js'

type CallContext = {
  telefuncFilePath: string
  telefunctionName: string
  telefunctionArgs: unknown[]
  stream?: { transport?: StreamTransport }
  channel: { transports: ChannelTransports }
  abortController: AbortController
  extensions?: Record<string, unknown>
  extensionRequestTypes: ReplacerType<TypeContract, ClientReplacerContext>[]
  connectionKey?: string
  channelIdleTimeout?: number
  headers?: Record<string, string> | null
  telefuncUrl: string
}

type SerializeResult = {
  httpRequestBody: string | Blob
  requestCloseHandlers: CloseHandler[]
}

/** Arguments that hold a value carried over a channel, or an extension's, serialized at the call's invocation before the
 *  page loaded channels: each such value is held in `held`, named in `serialized` by `SERIALIZER_PREFIX_HELD`. */
type HeldArguments = {
  held: unknown[]
  serialized: string
  files: Blob[]
  requestCloseHandlers: CloseHandler[]
}

const SERIALIZER_PREFIX_HELD = '!TelefuncHeld:'

/** Serializes the call's arguments, at its invocation; `HeldArguments` where they hold a value carried over a channel,
 *  or an extension's, and channels aren't loaded. */
function serializeTelefunctionArguments(callContext: CallContext): SerializeResult | HeldArguments {
  const dataMain: Record<string, unknown> = {
    file: callContext.telefuncFilePath,
    name: callContext.telefunctionName,
    args: callContext.telefunctionArgs,
  }

  if (callContext.stream?.transport) {
    const { transport } = callContext.stream
    dataMain.stream = { transport }
  }

  if (callContext.extensions) {
    dataMain.extensions = callContext.extensions
  }

  const wants = loadedFeatures().broadcasts?.unconfirmedWants(callContext.telefuncUrl)
  if (wants) dataMain.wants = wants

  const args: HeldArguments = { held: [], serialized: '', files: [], requestCloseHandlers: [] }
  const replacer = createCallReplacer(
    args,
    callContext,
    loadedFeatures().channels ? null : (value) => `${SERIALIZER_PREFIX_HELD}${args.held.push(value) - 1}`,
  )
  try {
    args.serialized = serialize(dataMain, callContext, replacer)
    return args.held.length > 0 ? args : requestBody(args)
  } catch (err) {
    cancelUnsentArguments(args, callContext, err)
    throw err
  }
}

/** Replaces the values `args` held, once channels are loaded. What else the call carries is read from what was
 *  serialized at its invocation, never from the app's objects again. */
function serializeHeldArguments(args: HeldArguments, callContext: CallContext): SerializeResult {
  const dataMain = parse(args.serialized, {
    reviver(_path, value) {
      if (value.startsWith(SERIALIZER_PREFIX_HELD)) {
        return { replacement: new Held(args.held[Number(value.slice(SERIALIZER_PREFIX_HELD.length))]) }
      }
      // A file replaced at the invocation stays as it was serialized: its index is in `args.files` already.
      if (value.startsWith(SERIALIZER_PREFIX_FILE) || value.startsWith(SERIALIZER_PREFIX_BLOB)) {
        return { replacement: new Verbatim(value) }
      }
      return undefined
    },
  })
  const replacer = createCallReplacer(args, callContext, null)
  args.serialized = serialize(dataMain, callContext, (key, value, serializer) => {
    if (value instanceof Verbatim) return { replacement: value.serialized, resolved: true }
    if (value instanceof Held) return replacer(key, value.value, serializer)
    return undefined
  })
  return requestBody(args)
}

/** Cancels the replacements and held uploads of a call that failed before it was sent. */
function cancelUnsentArguments(args: HeldArguments, callContext: CallContext, reason: unknown): void {
  callContext.abortController.abort()
  for (const value of args.held) {
    // A stream the app holds locked rejects its cancel, which is the app's.
    if (value instanceof ReadableStream) value.cancel(reason).catch(() => {})
  }
}

class Held {
  constructor(readonly value: unknown) {}
}
class Verbatim {
  constructor(readonly serialized: string) {}
}

function createCallReplacer(args: HeldArguments, callContext: CallContext, hold: ((value: unknown) => string) | null) {
  const connection = getConnectionOptions(callContext)
  const signal = callContext.abortController.signal
  return createRequestReplacer(
    {
      registerFile(body) {
        const index = args.files.length
        args.files.push(body)
        return index
      },
      createChannel(opts) {
        const { ClientChannel } = loadedFeature('channels')
        return new ClientChannel({ channelId: randomUuid(), ack: opts?.ack, connection })
      },
      sendStream(createProducer) {
        return loadedFeature('channels').pumpClientProducerToChannel(createProducer, connection)
      },
    },
    ({ value, close, abort }) => {
      const onAbort = () => abort(makeAbortError(undefined, callContext))
      if (signal.aborted) onAbort()
      else signal.addEventListener('abort', onAbort, { once: true })
      args.requestCloseHandlers.push(close)

      // The app's original value is the GC anchor.
      assert(isObjectOrFunction(value))
      getGcRegistry().register(value, close)
    },
    callContext.extensionRequestTypes,
    hold,
  )
}

function requestBody(args: HeldArguments): SerializeResult {
  const httpRequestBody = args.files.length > 0 ? encodeRequestEnvelope(args.serialized, args.files) : args.serialized
  return { httpRequestBody, requestCloseHandlers: args.requestCloseHandlers }
}

type Replacer = Parameters<typeof stringify>[1] extends infer O ? (O extends { replacer?: infer R } ? R : never) : never
function serialize(dataMain: unknown, callContext: CallContext, replacer?: Replacer): string {
  let serialized: string
  try {
    serialized = stringify(dataMain, { forbidReactElements: true, replacer })
  } catch (err) {
    assert(hasProp(err, 'message', 'string'))
    assertUsage(
      false,
      [
        `Cannot serialize arguments for telefunction ${callContext.telefunctionName}() (${callContext.telefuncFilePath}).`,
        'Make sure that the arguments passed to telefunction calls are always serializable.',
        `Serialization error: ${lowercaseFirstLetter(err.message)}`,
      ].join(' '),
    )
  }
  assert(serialized)
  return serialized
}
