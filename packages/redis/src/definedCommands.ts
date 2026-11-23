// The scripts `defineCommand` attaches to the backend's client, typed as ioredis documents for a defined command: by
// augmenting its `RedisCommander`, which a standalone client and a Cluster share. Each name of the commands table is
// a method taking the keys and arguments in one array, and so is its `Buffer` form, whose reply holds bytes. Only
// RedisConnection.ts imports it, as types, which leaves no import in the emitted code or declarations: an app's ioredis
// types don't gain these methods.
import type { ClientContext, Result } from 'ioredis'
import type { CommandName } from './commands.js'

type DefinedCommands<Context extends ClientContext> = {
  [Name in CommandName | `${CommandName}Buffer`]: (
    keysAndArgs: ReadonlyArray<string | Uint8Array>,
  ) => Result<unknown, Context>
}

declare module 'ioredis' {
  interface RedisCommander<Context extends ClientContext = { type: 'default' }> extends DefinedCommands<Context> {}
}
