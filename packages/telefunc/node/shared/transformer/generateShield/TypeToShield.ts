type Equals<X, Y> = (<T>() => T extends X ? 1 : 2) extends <T>() => T extends Y ? 1 : 2 ? true : false

type UnionToIntersection<T> = (T extends T ? (params: T) => any : never) extends (params: infer P) => any ? P : never

// prettier-ignore
// biome-ignore format:
type UnionToTuple<T, Res extends any[] = []> = UnionToIntersection<
  T extends any ? () => T : never
> extends () => infer ReturnType
  ? UnionToTuple<Exclude<T, ReturnType>, [...Res, ReturnType]>
  : Res

type ShieldRes<S extends string, Acc extends string[] = []> = {
  str: S
  acc: Acc
}

/** Extensions augment this via `declare global { namespace Telefunc { interface ShieldTypeMap { foo: Foo } } }`. */
declare global {
  namespace Telefunc {
    interface ShieldTypeMap {}
  }
}

// On multi-match (e.g. `Subject<T>` matches both `subject` and `observable` since
// Subject extends Observable), pick the key whose value type isn't a proper supertype
// of another matched key's value — the most specific.
// prettier-ignore
// biome-ignore format:
type ExtensionType<T, Acc extends any[] = [], Keys = keyof Telefunc.ShieldTypeMap> =
  Keys extends infer K extends keyof Telefunc.ShieldTypeMap & string
    ? T extends Telefunc.ShieldTypeMap[K]
      ? true extends HasStricterMatch<T, K>
        ? never
        : ShieldRes<`__telefunc_t.${K}`, Acc>
      : never
    : false
// prettier-ignore
// biome-ignore format:
type HasStricterMatch<T, K extends keyof Telefunc.ShieldTypeMap> =
  keyof Telefunc.ShieldTypeMap extends infer K2
    ? K2 extends keyof Telefunc.ShieldTypeMap
      ? K2 extends K ? false
        : T extends Telefunc.ShieldTypeMap[K2]
          ? Telefunc.ShieldTypeMap[K2] extends Telefunc.ShieldTypeMap[K]
            ? Telefunc.ShieldTypeMap[K] extends Telefunc.ShieldTypeMap[K2] ? false : true
            : false
          : false
      : false
    : false
type ExtensionTypeResult<T, Acc extends any[] = []> = [ExtensionType<T, Acc>] extends [never]
  ? false
  : ExtensionType<T, Acc>

// prettier-ignore
// biome-ignore format:
type SimpleType<T, Acc extends any[] = []> = [T] extends [never]
  ? ShieldRes<'__telefunc_t.never', Acc>
  : Equals<T, any> extends true
  ? ShieldRes<'__telefunc_t.any', Acc>
  : unknown extends T
  ? ShieldRes<'__telefunc_t.any', Acc>
  : Equals<T, string> extends true
  ? ShieldRes<'__telefunc_t.string', Acc>
  : Equals<T, number> extends true
  ? ShieldRes<'__telefunc_t.number', Acc>
  : Equals<T, boolean> extends true
  ? ShieldRes<'__telefunc_t.boolean', Acc>
  : Equals<T, void> extends true
  ? ShieldRes<'__telefunc_t.const(undefined)', Acc>
  : Equals<T, undefined> extends true
  ? ShieldRes<'__telefunc_t.const(undefined)', Acc>
  : Equals<T, Date> extends true
  ? ShieldRes<'__telefunc_t.date', Acc>
  : Equals<T, File> extends true
  ? ShieldRes<'__telefunc_t.file', Acc>
  : Equals<T, Blob> extends true
  ? ShieldRes<'__telefunc_t.blob', Acc>
  : Equals<T, ReadableStream<Uint8Array>> extends true
  ? ShieldRes<'__telefunc_t.readableStream', Acc>
  : Equals<T, ReadableStream> extends true
  ? ShieldRes<'__telefunc_t.readableStream', Acc>
  : T extends (...args: any[]) => any
  ? ShieldRes<'__telefunc_t.function', Acc>
  : ExtensionTypeResult<T, Acc> extends ShieldRes<any, any>
  ? ExtensionTypeResult<T, Acc>
  : false

// prettier-ignore
// biome-ignore format:
type ReplaceAll<S extends string, From extends string, To extends string> = From extends ''
  ? S
  : S extends `${infer Before}${From}${infer After}`
  ? `${Before}${To}${ReplaceAll<After, From, To>}`
  : S

// prettier-ignore
// biome-ignore format:
type Literal<T, Acc extends any[]> = T extends string
  ? ShieldRes<`__telefunc_t.const('${ReplaceAll<T, "'", "\\'">}')`, Acc>
  : T extends boolean
  ? ShieldRes<`__telefunc_t.const(${T})`, Acc>
  : T extends number
  ? ShieldRes<`__telefunc_t.const(${T})`, Acc>
  : false

type Joined<T extends any[], Acc extends any[], List = ShieldList<T>, J = JoinShieldResList<List>> = J extends string
  ? ShieldRes<J, Acc>
  : never

// prettier-ignore
// biome-ignore format:
type ArrayLike<T extends any[], Acc extends any[] = []> = T extends [...infer U]
  ? Equals<U['length'], number> extends true
    ? T extends (infer V)[]
      ? Shield<V, ['array', ...Acc]>
      : never
    : ShieldList<U> extends ShieldRes<any, any>[]
    ? Joined<U, ['tuple', ...Acc]>
    : never
  : never

type JoinRecord<T extends Record<string, ShieldRes<any, any>>, Acc extends any[]> = ShieldRes<
  JoinStrings<
    UnionToTuple<
      {
        [K in Extract<keyof T, string>]: `${K}: ${WrapShieldRes<T[K]>}`
      }[Extract<keyof T, string>]
    >
  >,
  ['{}', ...Acc]
>

type ShieldRecord<T extends Record<string, any>> = {
  [K in Extract<keyof T, string> as K]: Shield<T[K]>
}

// prettier-ignore
// biome-ignore format:
type KeyValueShieldRes<T extends Record<string, any>, Acc extends any[]> = ShieldRecord<T> extends Record<any, any>
  ? JoinRecord<ShieldRecord<T>, Acc>
  : never

// prettier-ignore
// biome-ignore format:
type KeyValueOrObjectShield<T extends Record<string, any>, Acc extends any[]> = T extends Record<infer K, infer V>
  ? Equals<K, string> extends true
    ? Shield<V, ['object', ...Acc]>
    : KeyValueShieldRes<T, Acc>
  : KeyValueShieldRes<T, Acc>

// prettier-ignore
// biome-ignore format:
type Shield<T, Acc extends any[] = []> = SimpleType<T> extends ShieldRes<any>
  ? SimpleType<T, Acc>
  : undefined extends T
  ? Shield<Exclude<T, undefined>, ['optional', ...Acc]>
  : null extends T
  ? Shield<Exclude<T, null>, ['nullable', ...Acc]>
  : IsUnion<T> extends true
  ? ShieldUnion<T, Acc>
  : T extends any[]
  ? ArrayLike<T, Acc>
  : T extends Record<any, any>
  ? KeyValueOrObjectShield<T, Acc>
  : [Literal<T, Acc>] extends [ShieldRes<any, any>]
  ? Literal<T, Acc>
  : never

// prettier-ignore
// biome-ignore format:
type ShieldUnion<T, Acc extends any[]> = ShieldList<UnionToTuple<T>> extends ShieldRes<any, any>[]
  ? Joined<UnionToTuple<T>, ['union', ...Acc]>
  : never

type IsUnion<T> = [T] extends [UnionToIntersection<T>] ? false : true

// prettier-ignore
// biome-ignore format:
type Wrap<T extends string, Keyword> = Keyword extends 'nullable'
  ? `__telefunc_t.nullable(${T})`
  : Keyword extends 'optional'
  ? `__telefunc_t.optional(${T})`
  : Keyword extends 'tuple'
  ? `__telefunc_t.tuple(${T})`
  : Keyword extends 'array'
  ? `__telefunc_t.array(${T})`
  : Keyword extends '{}'
  ? `{ ${T} }`
  : Keyword extends 'union'
  ? `__telefunc_t.or(${T})`
  : Keyword extends 'object'
  ? `__telefunc_t.object(${T})`
  : T

// prettier-ignore
// biome-ignore format:
type WrapShieldRes<T extends ShieldRes<any, any>> = T extends ShieldRes<infer S, infer Acc>
  ? Acc extends [infer Head, ...infer Tail]
    ? Tail extends any[]
      ? WrapShieldRes<ShieldRes<Wrap<S, Head>, Tail>>
      : never
    : S
  : never

type Sep<T> = T extends '' ? '' : ', '

type JoinShieldResList<T, Acc extends string = ''> = T extends [infer Head, ...infer Tail]
  ? Head extends ShieldRes<any, any>
    ? Tail extends ShieldRes<any, any>[]
      ? JoinShieldResList<Tail, `${Acc}${Sep<Acc>}${WrapShieldRes<Head>}`>
      : Acc
    : Acc
  : Acc

type ShieldList<T extends any[]> = Head<T> extends never ? [] : [Shield<Head<T>>, ...ShieldList<Tail<T>>]

type JoinStrings<T extends any[], Acc extends string = ''> = T extends [infer Head, ...infer Tail]
  ? Head extends string
    ? JoinStrings<Tail, `${Acc}${Sep<Acc>}${Head}`>
    : Acc
  : Acc

export type ShieldStr<T, Res = Shield<T>> = Res extends ShieldRes<any, any> ? WrapShieldRes<Res> : never

type Tail<L extends any[]> = L extends readonly [] ? [] : L extends readonly [any?, ...infer LTail] ? LTail : []

type Head<L extends any[]> = L['length'] extends 0 ? never : L[0]

type ShieldStrMap<T extends any[]> = {
  [K in keyof T]-?: T[K] extends undefined ? never : ShieldStr<T[K]>
}

type ShieldArrStr<T extends any[], M = ShieldStrMap<T>> = M extends any[] ? `[${JoinStrings<M>}]` : never

export type TypeToShield<T> = T extends (...args: any) => any
  ? ShieldArrStr<Parameters<T>>
  : // export isn't a function => do nothing
    'NON_FUNCTION_EXPORT'

import type { TELEFUNC_SHIELDS } from './shield-key.js'

/** Pull a named field out of a type's `[TELEFUNC_SHIELDS]` slot. Identity-matches on the
 *  symbol so unrelated symbol-keyed members can't accidentally satisfy the extraction. */
export type ShieldField<T, K extends string> = T extends { [TELEFUNC_SHIELDS]: { [P in K]: infer V } } ? V : never

/** Single-property type read once at setup to resolve `[TELEFUNC_SHIELDS]`'s exact escaped
 *  property name (`__@TELEFUNC_SHIELDS@<id>`, where `<id>` is per-project). */
export type _TelefuncShieldsKeyProbe = { [TELEFUNC_SHIELDS]: never }

type _test = [
  Expect<Equals<ShieldStr<string>, '__telefunc_t.string'>>,
  Expect<Equals<ShieldStr<number>, '__telefunc_t.number'>>,
  Expect<Equals<ShieldStr<boolean>, '__telefunc_t.boolean'>>,
  Expect<Equals<ShieldStr<Date>, '__telefunc_t.date'>>,
  Expect<Equals<ShieldStr<any>, '__telefunc_t.any'>>,
  // `void` and `undefined` both assert "runtime value is `undefined`" — the shield must enforce
  // the TS type at runtime. A callback typed `() => void` returning `true` would otherwise slip
  // through unchecked and violate caller assumptions (e.g. `if (await cb()) …`).
  Expect<Equals<ShieldStr<void>, '__telefunc_t.const(undefined)'>>,
  Expect<Equals<ShieldStr<undefined>, '__telefunc_t.const(undefined)'>>,
  // `never` = "no value is permitted." Emitted for e.g. `ChannelData<never>` when a direction
  // is disabled. Any value a non-TS client manages to send is rejected.
  Expect<Equals<ShieldStr<never>, '__telefunc_t.never'>>,
  Expect<Equals<ShieldStr<string | null>, '__telefunc_t.nullable(__telefunc_t.string)'>>,
  Expect<Equals<ShieldStr<string | undefined>, '__telefunc_t.optional(__telefunc_t.string)'>>,
  Expect<
    Equals<ShieldStr<number | null | undefined>, '__telefunc_t.optional(__telefunc_t.nullable(__telefunc_t.number))'>
  >,
  Expect<Equals<ShieldStr<1 | null>, '__telefunc_t.nullable(__telefunc_t.const(1))'>>,
  Expect<Equals<ShieldStr<1 | undefined>, '__telefunc_t.optional(__telefunc_t.const(1))'>>,
  Expect<Equals<ShieldStr<[number, boolean]>, '__telefunc_t.tuple(__telefunc_t.number, __telefunc_t.boolean)'>>,
  Expect<
    EqualsAnyOf<
      ShieldStr<[number?, boolean?]>,
      [
        '__telefunc_t.tuple(__telefunc_t.optional(__telefunc_t.number), __telefunc_t.optional(__telefunc_t.boolean))',
        '__telefunc_t.tuple(__telefunc_t.optional(__telefunc_t.boolean), __telefunc_t.optional(__telefunc_t.number))',
      ]
    >
  >,
  Expect<
    EqualsAnyOf<
      ShieldStr<'one' | 'two'>,
      [
        "__telefunc_t.or(__telefunc_t.const('two'), __telefunc_t.const('one'))",
        "__telefunc_t.or(__telefunc_t.const('one'), __telefunc_t.const('two'))",
      ]
    >
  >,
  Expect<
    EqualsAnyOf<
      ShieldStr<1 | 2 | undefined>,
      [
        '__telefunc_t.optional(__telefunc_t.or(__telefunc_t.const(2), __telefunc_t.const(1)))',
        '__telefunc_t.optional(__telefunc_t.or(__telefunc_t.const(1), __telefunc_t.const(2)))',
      ]
    >
  >,
  Expect<
    EqualsAnyOf<
      ShieldStr<number | string>,
      [
        '__telefunc_t.or(__telefunc_t.number, __telefunc_t.string)',
        '__telefunc_t.or(__telefunc_t.string, __telefunc_t.number)',
      ]
    >
  >,
  Expect<Equals<ShieldStr<number[]>, '__telefunc_t.array(__telefunc_t.number)'>>,
  Expect<
    Equals<
      ShieldStr<[number, string][]>,
      '__telefunc_t.array(__telefunc_t.tuple(__telefunc_t.number, __telefunc_t.string))'
    >
  >,
  Expect<
    Equals<ShieldStr<{ age: number; hasBike: boolean }>, '{ hasBike: __telefunc_t.boolean, age: __telefunc_t.number }'>
  >,
  Expect<
    Equals<
      ShieldStr<{ age: number; hasBike?: boolean }>,
      '{ hasBike: __telefunc_t.optional(__telefunc_t.boolean), age: __telefunc_t.number }'
    >
  >,
  Expect<Equals<ShieldStr<'test'>, "__telefunc_t.const('test')">>,
  Expect<Equals<ShieldStr<"'test'">, "__telefunc_t.const('\\'test\\'')">>,
  Expect<Equals<ShieldStr<true>, '__telefunc_t.const(true)'>>,
  Expect<Equals<ShieldStr<false>, '__telefunc_t.const(false)'>>,
  Expect<Equals<ShieldStr<123>, '__telefunc_t.const(123)'>>,
  Expect<
    Equals<
      ShieldStr<{ isGood: boolean } | string>,
      '__telefunc_t.or({ isGood: __telefunc_t.boolean }, __telefunc_t.string)'
    >
  >,
  Expect<Equals<ShieldStr<{ age: number }[]>, '__telefunc_t.array({ age: __telefunc_t.number })'>>,
  Expect<Equals<ShieldStr<Record<string, number>>, '__telefunc_t.object(__telefunc_t.number)'>>,
  Expect<
    Equals<
      ShieldStr<Record<string, { age?: number }>>,
      '__telefunc_t.object({ age: __telefunc_t.optional(__telefunc_t.number) })'
    >
  >,
  Expect<Equals<ShieldStr<Record<string, number>[]>, '__telefunc_t.array(__telefunc_t.object(__telefunc_t.number))'>>,
  Expect<Equals<ShieldArrStr<[string, number]>, '[__telefunc_t.string, __telefunc_t.number]'>>,
  Expect<Equals<ShieldArrStr<[number?]>, '[__telefunc_t.optional(__telefunc_t.number)]'>>,
  Expect<
    Equals<
      ShieldArrStr<[string, number?, string?]>,
      '[__telefunc_t.string, __telefunc_t.optional(__telefunc_t.number), __telefunc_t.optional(__telefunc_t.string)]'
    >
  >,
]
type Expect<T extends true> = T
type EqualsAnyOf<T, L extends any[]> = L extends [infer Head, ...infer Tail]
  ? Equals<T, Head> extends true
    ? true
    : EqualsAnyOf<T, Tail>
  : false
