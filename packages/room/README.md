# `@telefunc/room`

Rooms for Telefunc: members, presence, meta, messages and media tracks, on one server or across many.

## Install

```sh
npm install @telefunc/room
```

That's it — the Telefunc bundler plugin (Vite, webpack, Next.js, Babel) detects `@telefunc/room` in your dependencies and registers it automatically. Without a Telefunc bundler plugin, register it manually: `import '@telefunc/room/client'` in your client entry. The server needs no extra import: importing `Room` from `@telefunc/room` registers it.

## Usage

A telefunction returns the room, and the client joins it:

```ts
// Lobby.telefunc.ts
// Environment: server

import { Room } from '@telefunc/room'

export async function onGetLobby() {
  return await Room.getOrCreate('lobby')
}
```

```ts
// Lobby.ts
// Environment: client

import { onGetLobby } from './Lobby.telefunc'

const room = await onGetLobby()

room.subscribe((message, info, from) => console.log(`${from.meta.name}: ${message.text}`))

const me = await room.join({ meta: { name: 'Alice' } })
await me.publish({ text: 'hello' })
```

See [telefunc.com/room](https://telefunc.com/room) for the full documentation.
