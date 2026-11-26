export { serializeTelefunctionArguments }

import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../utils/assert.js'
import { hasProp } from '../../utils/hasProp.js'
import { lowercaseFirstLetter } from '../../utils/lowercaseFirstLetter.js'
import { createRequestReplacer, type Claim, type Replaced } from '../../wire-protocol/values/client/request/registry.js'
import { encodeRequestEnvelope } from '../../wire-protocol/wire/envelope.js'
import { loadFeatures, loadedFeatures, loadedFeature } from '../../wire-protocol/values/client/loadFeatures.js'
import { randomUuid } from '../../utils/randomUuid.js'
import { getConnectionOptions } from './getConnectionOptions.js'
import type { ConnectionOptions } from '../../wire-protocol/channel/client/ClientChannel.js'
import { isObjectOrFunction } from '../../utils/isObjectOrFunction.js'
import { makeAbortError, throwIfAborted } from './errors.js'
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
  fetch: typeof globalThis.fetch | null
}

/** The call's request, serialized at its invocation. `prepareChannels`: what it awaits before its POST where it claimed
 *  values carried over a channel of their own before the page loaded channels' code, to open their channels. */
type CallRequest = {
  httpRequestBody: string | Blob
  requestCloseHandlers: CloseHandler[]
  prepareChannels?: () => Promise<void>
}
type Claimed = { value: unknown; claim: Claim }

/** Serializes the call's arguments, once, at its invocation, into the body its request sends. */
function serializeTelefunctionArguments(callContext: CallContext): CallRequest {
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

  const files: Blob[] = []
  const request: CallRequest = { httpRequestBody: '', requestCloseHandlers: [] }
  let claimed: Claimed[] | undefined
  const connection = getConnectionOptions(callContext)
  const replacer = createRequestReplacer(
    {
      registerFile: (body) => files.push(body) - 1,
      createChannel(opts) {
        const { ClientChannel } = loadedFeature('channels')
        return new ClientChannel({ channelId: randomUuid(), ack: opts?.ack, connection })
      },
      sendStream(createProducer) {
        const { ClientChannel, pumpProducerToChannel } = loadedFeature('channels')
        const channel = new ClientChannel({ channelId: randomUuid(), connection })
        return { metadata: { channelId: channel.id }, ...pumpProducerToChannel(createProducer(), channel) }
      },
    },
    (replaced) => attach(request, callContext, replaced),
    (value, claim) => {
      const { channels } = loadedFeatures()
      if (channels) attach(request, callContext, { value, ...claim.open(channels, connection) })
      else (claimed ??= []).push({ value, claim })
    },
    callContext.extensionRequestTypes,
  )
  try {
    const serialized = serialize(dataMain, callContext, replacer)
    request.httpRequestBody = files.length > 0 ? encodeRequestEnvelope(serialized, files) : serialized
  } catch (err) {
    cancelUnsentCall(callContext, claimed, err)
    throw err
  }
  if (claimed) request.prepareChannels = channelsPreparation(claimed, request, connection, callContext)
  return request
}

/** Loads channels' code for the values a call claimed before the page had it, then opens their channels. */
function channelsPreparation(
  claimed: Claimed[],
  request: CallRequest,
  connection: ConnectionOptions,
  callContext: CallContext,
): () => Promise<void> {
  return async () => {
    try {
      await loadFeatures(['channels'], callContext)
      throwIfAborted(callContext)
    } catch (err) {
      cancelUnsentCall(callContext, claimed, err)
      throw err
    }
    const { channels } = loadedFeatures()
    assert(channels)
    for (const { value, claim } of claimed) attach(request, callContext, { value, ...claim.open(channels, connection) })
  }
}

/** Undoes a call that ends before it is sent: what it replaced or opened, through its controller, and what it claimed. */
function cancelUnsentCall(callContext: CallContext, claimed: Claimed[] | undefined, reason: unknown): void {
  callContext.abortController.abort()
  claimed?.forEach(({ claim }) => claim.release(reason))
}

/** Ties a value whose resources are live to its call: the call's abort aborts them, its end closes them, and so does
 *  the app dropping the value. */
function attach(request: CallRequest, callContext: CallContext, { value, close, abort }: Replaced): void {
  const { signal } = callContext.abortController
  const onAbort = () => abort(makeAbortError(undefined, callContext))
  if (signal.aborted) onAbort()
  else signal.addEventListener('abort', onAbort, { once: true })
  request.requestCloseHandlers.push(close)

  // The app's original value is the GC anchor.
  assert(isObjectOrFunction(value))
  getGcRegistry().register(value, close)
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
