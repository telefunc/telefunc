// Compile-time guard against Room barrel drift. Every type a Room user names is re-exported by a
// package entrypoint: `telefunc` (server) and `telefunc/client` (browser). This file references the
// full intended surface of each barrel, so dropping a public Room type from a barrel turns into a
// `pnpm test:types` failure (a namespace with no such member). The server side holds the whole
// surface — statics, guards, hooks; the client side lists exactly what a browser caller can reach.
//
// Type-only: the `import type * as` namespaces are erased, so nothing here runs (the server barrel's
// browser guard never fires) and the two entrypoints coexist in one file.
import type * as Server from 'telefunc'
import type * as Client from 'telefunc/client'

type Assert<T extends true> = T
type ChatMsg = { kind: 'chat'; text: string }
type TypedRoom = Server.Room<{ topic: string }, { name: string }, ChatMsg>
type TypedPublishData = Parameters<Server.LocalParticipant<{ name: string }, ChatMsg>['publish']>[0]
type TypedSubscribeData = Parameters<Parameters<TypedRoom['subscribe']>[0]>[0]
type _TypedSubscribe = Assert<TypedSubscribeData extends ChatMsg ? true : false>
type _TypedPublish = Assert<ChatMsg extends TypedPublishData ? true : false>
type _RejectNumber = Assert<number extends TypedPublishData ? false : true>
type _RequireText = Assert<{ kind: 'chat' } extends TypedPublishData ? false : true>

export type __RoomServerSurface = [
  Server.Room,
  Server.RoomInfo,
  Server.RoomOptions,
  Server.RoomMeta,
  Server.RoomGetOptions,
  Server.JoinOptions,
  Server.PublishOptions,
  Server.ParticipantMeta,
  Server.LocalParticipant,
  Server.RemoteParticipant,
  Server.Sender,
  Server.SendGuard,
  Server.PublishGuard,
  Server.JoinGuard,
  Server.AfterPublishHook,
  Server.AfterSendHook,
  Server.AfterJoinHook,
  Server.RoomSendReceipt,
  Server.RoomAckReceipt,
  Server.LeaveCause,
  Server.ParticipantRef,
  Server.BinaryFrameInfo,
  Server.BinaryPublishOptions,
  Server.RoomSnapshotView,
  Server.ParticipantSnapshotView,
]

export type __RoomClientSurface = [
  Client.Room,
  Client.RoomInfo,
  Client.RoomMeta,
  Client.JoinOptions,
  Client.PublishOptions,
  Client.ParticipantMeta,
  Client.LocalParticipant,
  Client.RemoteParticipant,
  Client.Sender,
  Client.RoomSendReceipt,
  Client.RoomAckReceipt,
  Client.LeaveCause,
  Client.BinaryFrameInfo,
  Client.BinaryPublishOptions,
  Client.RoomSnapshotView,
  Client.ParticipantSnapshotView,
]
