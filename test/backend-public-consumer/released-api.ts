// Compile-time compatibility snapshot for the released surface at
// fcd6fd507e6b8abe382ecd1e86520f37d7eeb320. This fixture is compiled under
// Node16, NodeNext and Bundler resolution by the public-consumer gate.
import {
  Broadcast,
  BroadcastChannel,
  ChannelClosedError,
  ChannelOverflowError,
  Channel,
  DefaultBroadcastAdapter,
  NetworkError,
  config,
  telefuncConfig,
  type BroadcastAdapter,
  type BroadcastTransport,
  type ChannelBase,
  type ClientChannel,
  type ReplacerType,
  type ReviverType,
  type ServerReplacerContext,
  type ServerReviverContext,
  type StreamingReplacerType,
  type TypeContract,
} from 'telefunc'
import { ConnectionError, withContext } from 'telefunc/client'
import { Telefunc as NodeTelefunc } from 'telefunc/node'
import { installRedis, RedisTransport, type InstallRedisOptions, type RedisBroadcastOptions } from '@telefunc/redis'

type Assert<T extends true> = T
type IsOptional<T, K extends keyof T> = {} extends Pick<T, K> ? true : false

type BroadcastAck = Awaited<ReturnType<InstanceType<typeof BroadcastChannel>['publish']>>
const _baseAck: BroadcastAck = { key: 'released', seq: 1, timestamp: 1 }

const _unsubscribe = () => {}
const _baseTransport: BroadcastTransport = {
  send: () => ({ seq: 1, timestamp: 1 }),
  listen: () => _unsubscribe,
  sendBinary: () => ({ seq: 1, timestamp: 1 }),
  listenBinary: () => _unsubscribe,
}
const _baseAdapter: BroadcastAdapter = {
  subscribe: () => _unsubscribe,
  publish: () => ({ seq: 1, timestamp: 1 }),
  subscribeBinary: () => _unsubscribe,
  publishBinary: () => ({ seq: 1, timestamp: 1 }),
}
const _defaultAdapter: BroadcastAdapter = new DefaultBroadcastAdapter(_baseTransport)

declare const _channelBase: ChannelBase
declare const _clientChannel: ClientChannel
declare const _contract: TypeContract
declare const _replacer: ReplacerType<TypeContract, ServerReplacerContext>
declare const _reviver: ReviverType<TypeContract, ServerReviverContext>
declare const _streamingReplacer: StreamingReplacerType<TypeContract, ServerReplacerContext>
const _passScopeRemainsOptional: Assert<IsOptional<ServerReplacerContext, 'passScope'>> = true

config.broadcast.transport = _baseTransport
const _sameConfig: typeof config = telefuncConfig
const _call = withContext(async () => 1, { channel: { idleTimeout: 0 } })

const _node = new NodeTelefunc()

declare const _redisClient: RedisBroadcastOptions['redis']
const _redisInstallOptions: InstallRedisOptions = { prefix: 'tf:' }
const _redisBroadcastOptions: RedisBroadcastOptions = { redis: _redisClient, prefix: 'tf:' }
const _redisTransport: BroadcastTransport = new RedisTransport(_redisBroadcastOptions)
installRedis(_redisClient, _redisInstallOptions)

void [
  Broadcast,
  Channel,
  _baseAck,
  _baseAdapter,
  _defaultAdapter,
  _channelBase,
  _clientChannel,
  _contract,
  _replacer,
  _reviver,
  _streamingReplacer,
  _passScopeRemainsOptional,
  _sameConfig,
  _call,
  _node,
  _redisTransport,
  ChannelClosedError,
  ChannelOverflowError,
  ConnectionError,
  NetworkError,
]
