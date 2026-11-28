# `@telefunc/room`

Rooms for Telefunc: members, presence, meta, messages and media tracks, on one server or across many.

## Install

```sh
npm install @telefunc/room
```

Telefunc's plugin loads Room from your app's `package.json`. For other dependency layouts, add `import '@telefunc/room/client'` to your client entry.

## Usage

A telefunction returns the room, and the client joins it:

```ts
// Lobby.telefunc.ts
// Environment: server

import { Room, type RoomMeta } from '@telefunc/room'

type MemberMeta = { name: string }
type Message = { text: string }

export async function onGetLobby() {
  const room = await Room.getOrCreate<RoomMeta, MemberMeta, Message>('lobby')
  Room.guard(room, { anyoneCanJoin: true })
  return room
}
```

```ts
// Lobby.ts
// Environment: client

import { onGetLobby } from './Lobby.telefunc'

const room = await onGetLobby()

room.subscribe((message, { from }) => console.log(from ? `${from.meta.name}: ${message.text}` : message.text))

const me = await room.join({ meta: { name: 'Alice' } })
await me.publish({ text: 'hello' })
```

See [telefunc.com/room](https://telefunc.com/room) for the full documentation.
