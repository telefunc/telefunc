// The released-API snapshot owns entrypoint symbol inventories; this fixture proves that Room's
// payload generic reaches both publishing and subscription without widening, narrowing, or `any`.
import type { LocalParticipant, Room } from 'telefunc'

type Assert<T extends true> = T
type IsAny<T> = 0 extends 1 & T ? true : false
type Exactly<Left, Right> =
  IsAny<Left | Right> extends true ? false : [Left, Right] extends [Right, Left] ? true : false

type ChatMsg = { kind: 'chat'; text: string }
type TypedRoom = Room<{ topic: string }, { name: string }, ChatMsg>
type Published = Parameters<LocalParticipant<{ name: string }, ChatMsg>['publish']>[0]
type Subscribed = Parameters<Parameters<TypedRoom['subscribe']>[0]>[0]

type _PublishIsExact = Assert<Exactly<Published, ChatMsg>>
type _SubscribeIsExact = Assert<Exactly<Subscribed, ChatMsg>>
