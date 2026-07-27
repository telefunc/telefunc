// Compile-time compatibility snapshot for the released surface at
// fcd6fd507e6b8abe382ecd1e86520f37d7eeb320. This fixture is compiled under
// Node16, NodeNext and Bundler resolution by the public-consumer gate.
import {
  Broadcast,
  BroadcastChannel,
  Channel,
  ChannelClosedError,
  ChannelOverflowError,
  NetworkError,
  config,
  telefuncConfig,
  type ChannelBase,
  type ClientChannel,
  type ReplacerType,
  type ReviverType,
  type ServerReplacerContext,
  type ServerReviverContext,
  type StreamingReplacerType,
  type TypeContract,
} from 'telefunc'
import {
  BACKEND_SPI_VERSION,
  disposeBackend,
  getBackend,
  HEAD_TRANSITIONS,
  installBackend,
  ORDERING_FRAME_LAYOUT,
  type BackendDriver,
  type BackendFactory,
  type BackendReceiver,
  type BackendSpi,
  type BackendSubscription,
  type BackendSubscriptionSource,
  type BroadcastLane,
  type LaneId,
  type PublishResult,
  type SubscriptionAttempt,
  type SubscriptionDriver,
} from 'telefunc/backend'
import { ConnectionError, withContext } from 'telefunc/client'
import { Telefunc as NodeTelefunc } from 'telefunc/node'
import { installRedis, RedisTransport, type InstallRedisOptions, type RedisBroadcastOptions } from '@telefunc/redis'

type Assert<T extends true> = T
type Extends<Actual, Baseline> = [Actual] extends [Baseline] ? true : false
type Compatible<Actual, Baseline> = Extends<Actual, Baseline> extends true ? Extends<Baseline, Actual> : false
type HasKeys<Actual, Keys extends PropertyKey> = Exclude<Keys, keyof Actual> extends never ? true : false
type IsOptional<T, K extends keyof T> = {} extends Pick<T, K> ? true : false

type Outbound = (data: string) => number
type Inbound = (data: number) => string
type ReleasedChannelBase = {
  readonly id: string
  readonly isClosed: boolean
  send(data: string): Promise<number>
  send(data: string, opts: { ack: true }): Promise<number>
  send(data: string, opts: { ack: false }): Promise<void>
  sendBinary(data: Uint8Array): Promise<void>
  sendBinary(data: Uint8Array, opts: { ack: true }): Promise<unknown>
  sendBinary(data: Uint8Array, opts: { ack: false }): Promise<void>
  listen(callback: (data: number) => string | Promise<string>): () => void
  listenBinary(callback: (data: Uint8Array) => unknown | Promise<unknown>): () => void
  onClose(callback: (error?: Error) => void | Promise<void>): void
  onOpen(callback: () => void): void
  close(opts?: { timeout?: number }): Promise<0 | 1>
  abort(): void
  abort(abortValue: unknown, message?: string): void
}
type _channelBaseShape = Assert<Compatible<ChannelBase<Outbound, Inbound, true>, ReleasedChannelBase>>
type _clientChannelShape = Assert<Extends<ClientChannel<Outbound, Inbound, true>, ReleasedChannelBase>>

type ReleasedPublishResult = {
  seq: number
  timestamp: number
  meta?: Record<string, unknown>
}
type ReleasedPublishAck = ReleasedPublishResult & { key: string }
type CurrentPublishAck = Awaited<ReturnType<InstanceType<typeof BroadcastChannel>['publish']>>
type _publishAckShape = Assert<Compatible<CurrentPublishAck, ReleasedPublishAck>>

type _backendKeys = Assert<
  HasKeys<
    BackendSpi,
    | 'spiVersion'
    | 'capabilities'
    | 'publish'
    | 'subscribe'
    | 'readHead'
    | 'compareExchangeHead'
    | 'readCells'
    | 'compareExchangeCells'
    | 'commitLane'
    | 'readRetained'
    | 'listRetained'
    | 'deleteRetained'
    | 'subscribeLane'
    | 'listGenerations'
    | 'dropGeneration'
    | 'directoryPut'
    | 'directoryDelete'
    | 'directoryList'
    | 'dispose'
  >
>
type _subscriptionKeys = Assert<HasKeys<BackendSubscription, 'ready' | 'state' | 'onStateChange' | 'unsubscribe'>>
type _attemptKeys = Assert<HasKeys<SubscriptionAttempt, 'ready' | 'state' | 'onStateChange' | 'unsubscribe'>>
type _driverKeys = Assert<HasKeys<SubscriptionDriver<string>, 'open'>>
type _driverOpenArguments = Assert<
  Compatible<Parameters<SubscriptionDriver<string>['open']>, [string, BackendReceiver, () => number]>
>
type _backendDriverKeys = Assert<
  HasKeys<
    BackendDriver,
    | 'spiVersion'
    | 'capabilities'
    | 'subscriptions'
    | 'publish'
    | 'readHead'
    | 'compareExchangeHead'
    | 'readCells'
    | 'compareExchangeCells'
    | 'commitLane'
    | 'readRetained'
    | 'listRetained'
    | 'deleteRetained'
    | 'listGenerations'
    | 'dropGeneration'
    | 'directoryPut'
    | 'directoryDelete'
    | 'directoryList'
    | 'dispose'
  >
>
type _sourceShape = Assert<
  Compatible<
    BackendSubscriptionSource,
    { kind: 'broadcast'; lane: BroadcastLane } | { kind: 'durable'; roomId: string; inc: string; lane: LaneId }
  >
>
type _transitionRowKeys = Assert<HasKeys<(typeof HEAD_TRANSITIONS)[number], 'from' | 'cx' | 'to' | 'constraint'>>
type _publishResultShape = Assert<Compatible<PublishResult, ReleasedPublishResult & { receivers?: number }>>
const orderingHeaderBytes: 16 = ORDERING_FRAME_LAYOUT.headerBytes
const orderingWordBytes: 4 = ORDERING_FRAME_LAYOUT.wordBytes
const orderingWordRange: 0x1_0000_0000 = ORDERING_FRAME_LAYOUT.wordRange
const orderingEndianness: 'big' = ORDERING_FRAME_LAYOUT.endianness
const orderingSeqHigh: 0 = ORDERING_FRAME_LAYOUT.offsets.seqHigh
const orderingSeqLow: 4 = ORDERING_FRAME_LAYOUT.offsets.seqLow
const orderingTimestampHigh: 8 = ORDERING_FRAME_LAYOUT.offsets.timestampHigh
const orderingTimestampLow: 12 = ORDERING_FRAME_LAYOUT.offsets.timestampLow
const backendVersion: 1 = BACKEND_SPI_VERSION
const broadcastLane: BroadcastLane = { key: 'released', kind: 'text' }
declare const backendFactory: BackendFactory
declare const backendDriver: BackendDriver
declare const backend: BackendSpi
const installedBackend: BackendSpi = installBackend(backendFactory)
const currentBackend: BackendSpi = getBackend()

type Contract = TypeContract<string, number, { token: string }>
type ReleasedContract = { value: string; result: number; metadata: { token: string } }
type _contractShape = Assert<Compatible<Contract, ReleasedContract>>
type Context = { origin: string }
type Replacer = ReplacerType<Contract, Context>
type Replacement = ReturnType<Replacer['replace']>
type _replacerKeys = Assert<HasKeys<Replacer, 'prefix' | 'detect' | 'replace'>>
type _replacerPrefix = Assert<Compatible<Replacer['prefix'], string>>
type _detectArguments = Assert<Compatible<Parameters<Replacer['detect']>, [unknown]>>
type _replaceArguments = Assert<Compatible<Parameters<Replacer['replace']>, [string, Context]>>
type _replacementKeys = Assert<HasKeys<Replacement, 'metadata' | 'close' | 'abort'>>
type _replacementMetadata = Assert<Compatible<Replacement['metadata'], { token: string }>>
type _replacementClose = Assert<Compatible<ReturnType<Replacement['close']>, Promise<void> | void>>
type Reviver = ReviverType<Contract, Context>
type Revival = ReturnType<Reviver['revive']>
type _reviverKeys = Assert<HasKeys<Reviver, 'prefix' | 'revive'>>
type _reviverPrefix = Assert<Compatible<Reviver['prefix'], string>>
type _reviveArguments = Assert<Compatible<Parameters<Reviver['revive']>, [{ token: string }, Context]>>
type _revivalKeys = Assert<HasKeys<Revival, 'value' | 'close' | 'abort'>>
type _revivedValue = Assert<Compatible<Revival['value'], number>>
type _revivalClose = Assert<Compatible<ReturnType<Revival['close']>, Promise<void> | void>>
type StreamingReplacer = StreamingReplacerType<Contract, Context>
type Producer = ReturnType<StreamingReplacer['createProducer']>
type _streamingReplacerKeys = Assert<HasKeys<StreamingReplacer, 'prefix' | 'detect' | 'replace' | 'createProducer'>>
type _createProducerArguments = Assert<Compatible<Parameters<StreamingReplacer['createProducer']>, [string]>>
type _producerKeys = Assert<HasKeys<Producer, 'chunks' | 'cancel'>>
type _producerChunks = Assert<Compatible<Producer['chunks'], AsyncIterator<Uint8Array<ArrayBuffer>>>>
type _producerCancelArguments = Assert<Compatible<Parameters<Producer['cancel']>, [reason?: unknown]>>

type _serverReplacerKeys = Assert<
  HasKeys<ServerReplacerContext, 'createChannel' | 'registerChannel' | 'sendStream' | 'validators' | 'passScope'>
>
type _passScopeRemainsOptional = Assert<IsOptional<ServerReplacerContext, 'passScope'>>
type _serverReviverKeys = Assert<
  HasKeys<ServerReviverContext, 'registerFile' | 'consumeFile' | 'createChannel' | 'receiveStream' | 'validators'>
>
type _registerFileArguments = Assert<Compatible<Parameters<ServerReviverContext['registerFile']>, [number, number]>>
type _consumeFileArguments = Assert<Compatible<Parameters<ServerReviverContext['consumeFile']>, [number, number]>>
type _receiveStreamArguments = Assert<
  Compatible<Parameters<ServerReviverContext['receiveStream']>, [{ channelId: string }]>
>
type ReceivedStream = ReturnType<ServerReviverContext['receiveStream']>
type _receivedStreamKeys = Assert<HasKeys<ReceivedStream, 'readNextChunk' | 'bytes' | 'stream' | 'cancel' | 'abort'>>

function pinServerContextMembers(replacer: ServerReplacerContext, reviver: ServerReviverContext): void {
  const channel = replacer.createChannel({ ack: true })
  replacer.registerChannel(channel)
  const sent = replacer.sendStream(() => ({
    chunks: (async function* () {
      yield new Uint8Array()
    })(),
    cancel: () => {},
  }))
  sent.close()
  reviver.registerFile(0, 0)
  reviver.consumeFile(0, 0)
  reviver.createChannel({ id: 'released', ack: true })
  const received = reviver.receiveStream({ channelId: 'released' })
  received.readNextChunk()
  received.bytes({ expectedSize: 0, onChunk: () => {}, cancelBehavior: 'close' })
  received.stream()
  received.cancel()
  declareAbortCompatibility(sent.abort, received.abort)
  void [replacer.validators, replacer.passScope, reviver.validators, sent.metadata]
}

function declareAbortCompatibility(replacementAbort: Replacement['abort'], streamAbort: ReceivedStream['abort']): void {
  void [replacementAbort, streamAbort]
}

const sameConfig: typeof config = telefuncConfig
const call = withContext(async () => 1, {
  signal: new AbortController().signal,
  headers: { Priority: 'u=0' },
  telefuncUrl: '/_telefunc',
  stream: { transport: 'binary-inline' },
  channel: { transports: ['sse'], connectionKey: 'released', idleTimeout: 0 },
  extensions: { released: { enabled: true } },
})

const node = new NodeTelefunc()
node.serve({ request: new Request('http://localhost/_telefunc') })

type _installRedisOptionKeys = Assert<HasKeys<InstallRedisOptions, 'prefix'>>
type _redisBroadcastOptionKeys = Assert<HasKeys<RedisBroadcastOptions, 'redis' | 'prefix'>>
declare const redisClient: RedisBroadcastOptions['redis']
const redisInstallOptions: InstallRedisOptions = { prefix: 'tf:' }
const redisBroadcastOptions: RedisBroadcastOptions = { redis: redisClient, prefix: 'tf:' }
const redisTransport = new RedisTransport(redisBroadcastOptions)
installRedis(redisClient, redisInstallOptions)

void [
  Broadcast,
  Channel,
  backendVersion,
  broadcastLane,
  backend,
  backendDriver,
  orderingHeaderBytes,
  orderingWordBytes,
  orderingWordRange,
  orderingEndianness,
  orderingSeqHigh,
  orderingSeqLow,
  orderingTimestampHigh,
  orderingTimestampLow,
  installedBackend,
  currentBackend,
  disposeBackend,
  HEAD_TRANSITIONS,
  sameConfig,
  call,
  node,
  redisTransport,
  pinServerContextMembers,
  ChannelClosedError,
  ChannelOverflowError,
  ConnectionError,
  NetworkError,
]
