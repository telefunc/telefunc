// Room's generics reach every public server/client view without widening, narrowing, bottoming out, or `any`.
import type * as Server from '@telefunc/room'
import type * as Client from '@telefunc/room/client'
import type { onGetChat } from './Chat.telefunc'

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
type ServerRemote = NonNullable<ReturnType<ServerRoom['getParticipant']>>

type _ServerSurface = AssertAll<
  [
    Exactly<ServerRoom['meta'], RoomMeta>,
    Exactly<NonNullable<Parameters<ServerRoom['join']>[0]>, Server.JoinOptions<MemberMeta>>,
    Exactly<ServerLocal, Server.LocalParticipant<MemberMeta, ChatMsg>>,
    Exactly<ServerLocal['meta'], MemberMeta>,
    Exactly<Parameters<ServerLocal['publish']>[0], ChatMsg>,
    Exactly<ServerRemote, Server.RemoteParticipant<MemberMeta, ChatMsg>>,
    Exactly<ReturnType<ServerRoom['getParticipant']>, ServerRemote | null>,
    Exactly<ReturnType<ServerRoom['getParticipants']>, ServerRemote[]>,
    Exactly<ServerRemote['meta'], MemberMeta>,
    Exactly<Parameters<Parameters<ServerRemote['subscribe']>[0]>[0], ChatMsg>,
    Exactly<Parameters<Parameters<ServerRoom['subscribe']>[0]>[0], ChatMsg>,
    Exactly<Parameters<Parameters<ServerRoom['subscribe']>[0]>[1]['from'], Server.Sender<MemberMeta> | null>,
    Exactly<ReturnType<ServerRoom['snapshot']>, Server.RoomSnapshotView<RoomMeta, MemberMeta, ChatMsg>>,
    Exactly<ReturnType<ServerRoom['snapshot']>['participants'][number], ServerRemote>,
    Exactly<ReturnType<ServerRoom['messages']>, AsyncIterableIterator<Server.RoomMessage<ChatMsg, MemberMeta>>>,
  ]
>

// A page's room is the type its telefunction returns, which is the room `@telefunc/room/client` names.
type _ClientSurface = AssertAll<
  [
    Exactly<ClientRoom, ServerRoom>,
    Exactly<Awaited<ReturnType<typeof onGetChat>>, Client.Room<Record<string, never>, { name: string }>>,
  ]
>

// It takes what the server's room takes; the page refuses the server-only options at run time.
function assertPageRoomTakesServerOptions(room: ClientRoom) {
  void room.join({ meta: { name: 'Alice' }, identity: 'user-1', hidden: true })
  void room.getParticipants({ hidden: true })
}

function assertTrackInfo(room: ClientRoom, member: ServerRemote) {
  room.subscribe((_message, info) => void info.track, { track: 'cursor' })
  member.subscribe((_message, info) => void info.meta, { track: 'cursor' })
  // @ts-expect-error a message on no track has no track in its info
  room.subscribe((_message, info) => void info.track)
  // @ts-expect-error a message on no track has no track in its info
  member.subscribe((_message, info) => void info.track)
}
