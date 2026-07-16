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
  Server.RoomPublishReceipt,
  Server.RoomSendReceipt,
  Server.RoomAckReceipt,
  Server.RoomJoinReceipt,
  Server.LeaveCause,
  Server.ParticipantRef,
  Server.BinaryFrameInfo,
  Server.BinaryPublishOptions,
  Server.RoomSnapshotView,
  Server.ParticipantSnapshotView,
  Server.RoomListener,
  Server.RoomBinaryListener,
  Server.ParticipantListener,
  Server.ParticipantBinaryListener,
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
  Client.RoomListener,
  Client.RoomBinaryListener,
  Client.ParticipantListener,
  Client.ParticipantBinaryListener,
]
