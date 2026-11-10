export type { OrmDriver, ReadArgs, WriteArgs }

/** What a subscriber's query depends on. */
type ReadArgs = { user: number; user2: number; team: number; low: number; high: number }
/** Random values a write uses: an existing todo, tag and user, and a fresh one. */
type WriteArgs = { todo: number; tag: number; user: number; otherUser: number; value: number; text: string; date: Date }

/** The oracle's query shapes and write mix, through one ORM. A shape an ORM can't express is left out. */
type OrmDriver = {
  reads: Record<string, (args: ReadArgs) => Promise<unknown>>
  writes: Record<string, (args: WriteArgs) => Promise<unknown>>
  close(): Promise<void>
}
