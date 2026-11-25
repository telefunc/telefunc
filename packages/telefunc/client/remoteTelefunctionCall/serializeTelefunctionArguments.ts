export { serializeTelefunctionArguments, serializeHeldArguments, cancelHeldArguments }
export type { HeldArguments }

import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { assert, assertUsage } from '../../utils/assert.js'
import { hasProp } from '../../utils/hasProp.js'
import { lowercaseFirstLetter } from '../../utils/lowercaseFirstLetter.js'
import { createRequestReplacer } from '../../wire-protocol/values/client/request/registry.js'
import { encodeRequestEnvelope } from '../../wire-protocol/wire/envelope.js'
import { loadedFeatures, loadedChannels } from '../../wire-protocol/values/client/loadFeatures.js'
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

type Replaced = Parameters<Parameters<typeof createRequestReplacer>[1]>[0]

type SerializeResult = {
  httpRequestBody: string | Blob
  requestCloseHandlers: CloseHandler[]
}

/** Arguments that hold a value carried over a channel, serialized at the call's invocation before the page loaded
 *  channels: each such value is held in `held`, named in `serialized` by `SERIALIZER_PREFIX_HELD`. */
type HeldArguments = {
  held: unknown[]
  serialized: string
  files: Blob[]
  replaced: Replaced[]
}

// A held token ends in `!`, which cannot end the JSON payload of a wire tag.
const SERIALIZER_PREFIX_HELD = '!TelefuncHeld:'

/** Serializes the call's arguments, at its invocation; `HeldArguments` where they hold a value carried over a channel
 *  and channels aren't loaded. */
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

  const args: HeldArguments = { held: [], serialized: '', files: [], replaced: [] }
  const replacer = createRequestReplacer(
    createReplacerContext(args.files, callContext),
    (replaced) => args.replaced.push(replaced),
    callContext.extensionRequestTypes,
    loadedFeatures().channels ? null : (value) => `${SERIALIZER_PREFIX_HELD}${args.held.push(value) - 1}!`,
  )
  args.serialized = serialize(dataMain, callContext, replacer)
  if (args.held.length > 0) return args
  return withLifecycles(args, callContext)
}

/** Replaces the values `args` held, once channels are loaded. What else the call carries is read from what was
 *  serialized at its invocation, never from the app's objects again. */
function serializeHeldArguments(args: HeldArguments, callContext: CallContext): SerializeResult {
  const replacedPrefixes = [
    SERIALIZER_PREFIX_FILE,
    SERIALIZER_PREFIX_BLOB,
    ...callContext.extensionRequestTypes.map((type) => type.prefix),
  ]
  const dataMain = parse(args.serialized, {
    reviver(_path, value) {
      if (value.startsWith(SERIALIZER_PREFIX_HELD) && value.endsWith('!')) {
        return { replacement: new Held(args.held[Number(value.slice(SERIALIZER_PREFIX_HELD.length, -1))]) }
      }
      // A value replaced at the invocation stays as it was serialized: a file's index is in `args.files` already.
      if (replacedPrefixes.some((prefix) => value.startsWith(prefix))) return { replacement: new Verbatim(value) }
      return undefined
    },
  })
  const replacer = createRequestReplacer(
    createReplacerContext(args.files, callContext),
    (replaced) => args.replaced.push(replaced),
    callContext.extensionRequestTypes,
    null,
  )
  args.serialized = serialize(dataMain, callContext, (key, value, serializer) => {
    if (value instanceof Verbatim) return { replacement: value.serialized, resolved: true }
    if (value instanceof Held) return replacer(key, value.value, serializer)
    return undefined
  })
  return withLifecycles(args, callContext)
}

/** A held call that ends before it is sent cancels the streams it would have uploaded, as a sent one's abort does. */
function cancelHeldArguments(args: HeldArguments, reason: unknown): void {
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

function createReplacerContext(files: Blob[], callContext: CallContext): ClientReplacerContext {
  const connection = getConnectionOptions(callContext)
  return {
    registerFile(body) {
      const index = files.length
      files.push(body)
      return index
    },
    createChannel(opts) {
      const { ClientChannel } = loadedChannels()
      return new ClientChannel({ channelId: randomUuid(), ack: opts?.ack, connection })
    },
    sendStream(createProducer) {
      return loadedChannels().pumpClientProducerToChannel(createProducer, connection)
    },
  }
}

/** The body the call sends, with its values' lifecycles tied to the call. */
function withLifecycles(args: HeldArguments, callContext: CallContext): SerializeResult {
  const abortSignal = callContext.abortController.signal
  const requestCloseHandlers: CloseHandler[] = []
  for (const replaced of args.replaced) {
    {
      // Track the user's actual value — when they drop all references to it, close.
      // (Unlike the response side, we don't create a value here; the user already
      //  holds the original, so it serves as its own GC anchor.)
      const { value, close } = replaced
      assert(isObjectOrFunction(value))
      getGcRegistry().register(value, close)
    }

    {
      const { close, abort } = replaced
      abortSignal.addEventListener(
        'abort',
        () => {
          abort(makeAbortError(undefined, callContext))
        },
        { once: true },
      )
      requestCloseHandlers.push(close)
    }
  }
  const httpRequestBody = args.files.length > 0 ? encodeRequestEnvelope(args.serialized, args.files) : args.serialized
  return { httpRequestBody, requestCloseHandlers }
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
