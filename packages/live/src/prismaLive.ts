export { withLiveCollections }
export type { PrismaClient, PrismaTypes, Selected, WithLiveModels }

import { assertUsage } from './assert.js'
import type { Live } from './live.js'
import { readLive } from './liveQuery.js'

/** Prisma's types, by target: the entry of each target (`prisma.ts`, `prismaSqlite.ts`) adds its own, from the Prisma
 *  package of that target, the only one its app installs. Each installed copy of Prisma has symbols of its own. For a
 *  collection `C`: `hasRow`, what every collection is; `row`, the rows of `C`; `picked`, what `C.select(Field)`
 *  returns. */
interface PrismaTypes<C, Field> {}
type Types<C, Field, Target> = Target extends keyof PrismaTypes<C, Field> ? PrismaTypes<C, Field>[Target] : never
type HasRow<Target> = Types<unknown, unknown, Target>['hasRow']
type RowOf<C, Target> = Types<C, unknown, Target>['row']
type Picked<C, Field, Target> = Types<C, Field, Target>['picked']

type LiveMethod<Target> = {
  /** Runs the query like `all()` does, and also makes its result live: the client fetches it again when a committed
   *  write changes what it read. */
  live<Self extends HasRow<Target>>(this: Self): Promise<Live<RowOf<Self, Target>[]>>
}

// Prisma's collection class isn't exported, so its type can't take a method, and an interface it implements can't
// either: a class's type doesn't get what is merged into such an interface. `livePrisma()` gives each model's
// collection `live()`; the steps that return the collection they're called on (`where()`, `limit()`, `include()`, …)
// keep it, and `select()`, which returns a new collection type, is retyped to keep it too. `variant()` isn't: the rows
// it returns are typed by Prisma types the package doesn't export.

type PrismaClient = {
  orm: object
  contract: { roots: Record<string, { model: string; namespace: string }> }
}

/** `T`, with `.live()` at the end of its models' collections, its transactions' too. */
type WithLiveModels<T, Target> = T extends { orm: infer Orm }
  ? Omit<T, 'orm' | 'transaction'> & { readonly orm: LiveOrm<Orm, Target> } & (T extends {
        transaction(run: (tx: infer Tx) => PromiseLike<any>): Promise<any>
      }
        ? { transaction<R>(run: (tx: WithLiveModels<Tx, Target>) => PromiseLike<R>): Promise<R> }
        : {})
  : T
/** `db.orm`: by namespace on Postgres, the models themselves on SQLite. */
type LiveOrm<Orm, Target> = {
  [K in keyof Orm]: Orm[K] extends HasRow<Target> ? LiveCollection<Orm[K], Target> : LiveModels<Orm[K], Target>
}
type LiveModels<Models, Target> = Models extends (...args: any[]) => any
  ? Models
  : {
      [Model in keyof Models]: Models[Model] extends HasRow<Target>
        ? LiveCollection<Models[Model], Target>
        : Models[Model]
    }

/** A collection whose steps keep `live()`. */
type LiveCollection<C, Target> = Omit<C, 'select'> &
  LiveMethod<Target> & {
    // Typed by the collection it's called on, as Prisma's own `select()` is: the steps before it (`include()`,
    // `orderBy()`, …) typed its rows and its state.
    select<Self extends HasRow<Target>, Fields extends FieldsOf<C>>(
      this: Self,
      ...fields: Fields
    ): LiveCollection<Picked<Self, Fields[number], Target>, Target>
  }
type FieldsOf<C> = C extends { select(...fields: infer Fields): unknown } ? Fields : never

/** `Row` with only the model's `Field`s: its included relations stay. */
type Selected<Row, ModelRow, Field> = SimplifyDeep<Pick<ModelRow, Field & keyof ModelRow> & Omit<Row, keyof ModelRow>>
/** Prisma's own, which its packages don't export. */
type SimplifyDeep<T> = T extends readonly (infer Element)[]
  ? T extends unknown[]
    ? SimplifyDeep<Element>[]
    : readonly SimplifyDeep<Element>[]
  : T extends string | number | boolean | bigint | symbol | Date | RegExp | Uint8Array | ((...args: never[]) => unknown)
    ? T
    : T extends object
      ? { [K in keyof T]: SimplifyDeep<T[K]> }
      : T

/** Gives the collections of `db`'s models `live()`. */
function withLiveCollections(db: PrismaClient): void {
  // The class isn't exported: any model's collection gives its prototype.
  const [root] = Object.values(db.contract.roots)
  assertUsage(root, 'livePrisma() needs a contract with at least one model.')
  const orm = db.orm as Record<string, Record<string, object>>
  // On SQLite, `db.orm` holds the models themselves.
  const models = orm[root.namespace] ?? orm
  const collection = Object.getPrototypeOf(models[root.model]!)
  if (!('live' in collection))
    Object.defineProperty(collection, 'live', {
      value: function live(this: { all(): PromiseLike<unknown[]> }) {
        return readLive(async () => await this.all(), 'livePrisma()')
      },
    })
}
