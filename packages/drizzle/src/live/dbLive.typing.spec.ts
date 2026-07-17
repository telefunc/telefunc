import { boolean, integer, pgTable, text } from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/pglite'
import { it } from 'vitest'
import type { Live } from 'telefunc'

// A COMPILE-TIME contract, checked by the package's own `tsc --build`. It settles one question against
// REAL Drizzle types: can `db.live.select().from(todos)` be typed to `Live<Todo[]>` with the row type
// intact? The answer is no — for a structural reason, not a missing effort — and the wrap form can.
// Everything below either type-checks or is pinned with `@ts-expect-error`; if a pin ever stops
// erroring (someone found a way to type the dotted form), tsc fails here and this proof gets revisited.

const todos = pgTable('todos', {
  id: text('id').primaryKey(),
  title: text('title').notNull(),
  done: boolean('done').notNull(),
  rank: integer('rank').notNull(),
})
declare const baseDb: ReturnType<typeof drizzle>

// ── CONTROL: real Drizzle keeps the row type through select().from(). If this ever breaks, the failures
//    below are Drizzle's, not ours — so a green control is what makes the rest attributable to us. ──
async function control() {
  const rows = await baseDb.select().from(todos)
  const done: boolean = rows[0]!.done
  const title: string = rows[0]!.title
  void [done, title]
}

// ── THE DOTTED FORM, best case: mirror both `select` overloads so `select()` itself compiles, then
//    chain through a member-remap. This is the strongest version of `db.live.select().from()`. ──
type LiveOf<B> = B extends PromiseLike<infer R>
  ? { [K in keyof B]: LiveMember<B[K]> } & {
      then<T1 = Live<R>>(onf?: ((v: Live<R>) => T1 | PromiseLike<T1>) | null): Promise<T1>
    }
  : { [K in keyof B]: LiveMember<B[K]> }
type LiveMember<M> = M extends (...args: infer P) => infer Ret
  ? Ret extends { execute: (...a: any[]) => any }
    ? (...args: P) => LiveOf<Ret>
    : M
  : M
type LiveNamespace<TDb> = TDb extends { select: { (): infer RWhole; (fields: infer F): infer RProj } }
  ? { select(): LiveOf<RWhole>; select(fields: F): LiveOf<RProj> }
  : Record<never, never>

declare const dotted: LiveNamespace<typeof baseDb>
async function dottedFormDropsTheRowType() {
  const rows = await dotted.select().from(todos) // select() compiles — the overload is mirrored
  // But the row type is gone. `from` is GENERIC (`from<TFrom extends ...>`), and any conditional-type
  // remap of a generic method collapses it to its constraint — TypeScript has no way to re-quantify a
  // generic method's own type parameters. So `.data` is `{ [x: string]: any } | {}`, and `.done` is not
  // on it. This is the whole of why the dotted form cannot carry the row type.
  // @ts-expect-error — `done` does not exist on the collapsed row type
  const done: boolean = rows.data[0]!.done
  void done
}

// ── THE WRAP FORM: let Drizzle build the query with its OWN types, and wrap the finished PromiseLike.
//    `R` is inferred from a fully-typed Drizzle query, so nothing is remapped and nothing collapses. ──
declare function liveWrap<R>(query: PromiseLike<R>): Promise<Live<R>>
async function wrapFormKeepsTheRowType() {
  const rows = await liveWrap(baseDb.select().from(todos))
  const done: boolean = rows.data[0]!.done // resolves
  const title: string = rows.data[0]!.title
  void [done, title]
}
async function wrapFormKeepsAProjection() {
  const rows = await liveWrap(baseDb.select({ id: todos.id, done: todos.done }).from(todos))
  const id: string = rows.data[0]!.id
  const done: boolean = rows.data[0]!.done
  void [id, done]
}

void [control, dottedFormDropsTheRowType, wrapFormKeepsTheRowType, wrapFormKeepsAProjection]

// The proof IS the typecheck above; this keeps vitest from reporting an empty suite.
it('db.live typing contract holds at compile time', () => {})
