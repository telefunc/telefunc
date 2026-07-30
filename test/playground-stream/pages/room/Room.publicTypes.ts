// The released-API snapshot owns entrypoint symbol inventories; this fixture proves that Room's
// generics reach every public server/client view without widening, narrowing, bottoming out, or `any`.
import type * as Server from 'telefunc'
import type * as Client from 'telefunc/client'

type Assert<T extends true> = T
type AssertAll<T extends readonly true[]> = T
type IsAny<T> = 0 extends 1 & T ? true : false
type Exactly<Left, Right> = IsAny<Left | Right> extends true
  ? false
  : [Left, Right] extends [Right, Left]
    ? true
    : false

type RoomMeta = { topic: string }
type MemberMeta = { name: string }
type ChatMsg = { kind: 'chat'; text: string }
type ServerRoom = Server.Room<RoomMeta, MemberMeta, ChatMsg>
type ClientRoom = Client.Room<RoomMeta, MemberMeta, ChatMsg>
type ServerLocal = Awaited<ReturnType<ServerRoom['join']>>
type ClientLocal = Awaited<ReturnType<ClientRoom['join']>>
type ServerRemote = NonNullable<Awaited<ReturnType<ServerRoom['getParticipant']>>>
type ClientRemote = NonNullable<Awaited<ReturnType<ClientRoom['getParticipant']>>>

type _ServerSurface = AssertAll<
  [
    Exactly<ServerRoom['meta'], RoomMeta>,
    Exactly<NonNullable<Parameters<ServerRoom['join']>[0]>, Server.JoinOptions<MemberMeta>>,
    Exactly<ServerLocal, Server.LocalParticipant<MemberMeta, ChatMsg>>,
    Exactly<ServerLocal['meta'], MemberMeta>,
    Exactly<Parameters<ServerLocal['publish']>[0], ChatMsg>,
    Exactly<ServerRemote, Server.RemoteParticipant<MemberMeta, ChatMsg>>,
    Exactly<ServerRemote['meta'], MemberMeta>,
    Exactly<Parameters<Parameters<ServerRemote['subscribe']>[0]>[0], ChatMsg>,
    Exactly<Parameters<Parameters<ServerRoom['subscribe']>[0]>[0], ChatMsg>,
    Exactly<Parameters<Parameters<ServerRoom['subscribe']>[0]>[2], Server.Sender<MemberMeta>>,
    Exactly<Parameters<Parameters<ServerRoom['onUpdate']>[0]>[0], RoomMeta>,
    Exactly<Parameters<Parameters<ServerRoom['onParticipantUpdate']>[0]>[1], MemberMeta>,
    Exactly<ReturnType<ServerRoom['snapshot']>, Server.RoomSnapshotView<RoomMeta, MemberMeta>>,
    Exactly<ReturnType<ServerRoom['snapshot']>['participants'][number]['meta'], MemberMeta>,
  ]
>

type _ClientSurface = AssertAll<
  [
    Exactly<ClientRoom['meta'], RoomMeta>,
    Exactly<NonNullable<Parameters<ClientRoom['join']>[0]>, Client.JoinOptions<MemberMeta>>,
    Exactly<ClientLocal, Client.LocalParticipant<MemberMeta, ChatMsg>>,
    Exactly<ClientLocal['meta'], MemberMeta>,
    Exactly<Parameters<ClientLocal['publish']>[0], ChatMsg>,
    Exactly<ClientRemote, Client.RemoteParticipant<MemberMeta, ChatMsg>>,
    Exactly<ClientRemote['meta'], MemberMeta>,
    Exactly<Parameters<Parameters<ClientRemote['subscribe']>[0]>[0], ChatMsg>,
    Exactly<Parameters<Parameters<ClientRoom['subscribe']>[0]>[0], ChatMsg>,
    Exactly<Parameters<Parameters<ClientRoom['subscribe']>[0]>[2], Client.Sender<MemberMeta>>,
    Exactly<Parameters<Parameters<ClientRoom['onUpdate']>[0]>[0], RoomMeta>,
    Exactly<Parameters<Parameters<ClientRoom['onParticipantUpdate']>[0]>[1], MemberMeta>,
    Exactly<ReturnType<ClientRoom['snapshot']>, Client.RoomSnapshotView<RoomMeta, MemberMeta>>,
    Exactly<ReturnType<ClientRoom['snapshot']>['participants'][number]['meta'], MemberMeta>,
  ]
>

// Permanent false controls for the three escapes exercised against the public projections above.
type _ComparatorRejectsEscapes = AssertAll<
  [
    Exactly<never, ChatMsg> extends false ? true : false,
    Exactly<ChatMsg | { kind: 'system' }, ChatMsg> extends false ? true : false,
    Exactly<any, ChatMsg> extends false ? true : false,
  ]
>
