// THE routing identity of a relation — the single string both sides of the live pipeline agree on.
//
// A live query registers its graph under the identities it reads; a write emits TableChanges under the
// identity it touched, and the router matches them. Read-side and write-side identities are derived from
// the SAME drizzle table declaration, so they match by construction.
//
// SCHEMA-QUALIFIED, because a bare table name is not an identity: `a.users` and `b.users` are different
// physical relations that a bare name conflates, which routes a write on one to a live query on the other
// (a false invalidation of a query that is provably unaffected).
//
// ENCODING — two forms, disjoint by construction, so the mapping is INJECTIVE over all legal SQL
// identifiers (a table name may be any string, so "unlikely in practice" is not good enough):
//
//   plain   `users`        an UNQUALIFIED relation whose name is already unambiguous — used verbatim,
//                          which keeps diagnostics readable.
//   framed  `1:a5:users`   everything else — each part length-prefixed (utils/canonical `frame`), so
//                          `a`+`b.c` and `a.b`+`c` cannot collide the way a `.`-join would.
//
// A name is pushed into the framed form when it could be mistaken for one — it starts with `{digits}:`.
// The framed form always starts with `{digits}:` and the plain form never does, so no plain identity can
// forge a framed one, and vice versa.
//
// AMBIGUITY CLASS — a relation drizzle declares WITHOUT a schema (`pgTable('users', …)`, every sqliteTable)
// has no statically-knowable schema: the connection's search_path resolves it at runtime. Such a relation
// gets the UNQUALIFIED identity (empty schema) — deliberately the coarser identity class, shared by every
// unqualified declaration of that name. This is sound because both the read and the write derive their
// identity from the same table declaration. The residual limitation, stated honestly: declaring ONE physical
// table TWICE — once with an explicit schema and once without — and reading through one declaration while
// writing through the other yields two identities, so the write does not invalidate the read. Declare a
// relation once (the normal drizzle pattern) and the case cannot arise.

export { relationIdOf, parseRelationId, describeRelationId }

import { frame } from '../utils/canonical.js'

/** What an identity is computed from — the drizzle-free projection of a TableRef. */
type RelationName = { name: string; schema?: string }

/** True when a bare name would be indistinguishable from the framed form — such a name must take the
 *  framed form so the two encodings stay disjoint. */
function needsFraming(name: string): boolean {
  return /^\d+:/.test(name)
}

/** The routing identity of a relation. Aliases share it (they resolve to the real name). */
function relationIdOf(relation: RelationName): string {
  const { name, schema } = relation
  if (schema === undefined && !needsFraming(name)) return name
  return frame(schema ?? '') + frame(name)
}

/** Recover the schema/name an identity was built from — the inverse of {@link relationIdOf}, available
 *  because the framing is injective. Consumers that must address the relation in SQL (the RLS catalog
 *  probe) need the parts back, and re-deriving them from a drizzle Table is not always in reach. */
function parseRelationId(id: string): RelationName {
  if (!/^\d+:/.test(id)) return { name: id } // the plain form is its own name
  const schema = readFrame(id, 0)
  const name = schema && readFrame(id, schema.end)
  if (!schema || !name || name.end !== id.length) {
    throw new Error(`telefunc live: malformed relation identity ${JSON.stringify(id)}`)
  }
  return schema.text.length > 0 ? { name: name.text, schema: schema.text } : { name: name.text }
}

/** A relation identity as a human would write it (`a.users`, or `users` unqualified) — for diagnostics
 *  and error messages only, NEVER as a routing key: this rendering is lossy (it is not injective). */
function describeRelationId(id: string): string {
  const { name, schema } = parseRelationId(id)
  return schema ? `${schema}.${name}` : name
}

/** Read one `{length}:{text}` frame starting at `from`. */
function readFrame(text: string, from: number): { text: string; end: number } | undefined {
  const colon = text.indexOf(':', from)
  if (colon < 0) return undefined
  const digits = text.slice(from, colon)
  if (!/^\d+$/.test(digits)) return undefined
  const length = Number(digits)
  const end = colon + 1 + length
  if (end > text.length) return undefined
  return { text: text.slice(colon + 1, end), end }
}
