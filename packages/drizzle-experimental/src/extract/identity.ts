export { identityOf, planKeyOf, instanceKeyOf, canonicalValue }
export type { Identity, IdentityEnv }

import { Placeholder, fillPlaceholders, is } from 'drizzle-orm'
import { assertUsage } from '../utils/assert.js'
import { canonicalValue, frame } from '../ir/canonical.js'

type Identity = {
  /** The graph-state key: the structural plan key (semantic environment / dialect / schema
   *  fingerprint / SQL shape, via planKeyOf) folded together with the typed bindings. Same rows read
   *  under the same authority ⇒ same graph — this is the registry's identical-instance dedup key, and
   *  its structural foundation is what keeps a graph from being shared across an RLS/role/dialect
   *  boundary. */
  instanceKey: string
}

type IdentityEnv = {
  dialect: string
  /** From binding/database: the connection's proven semantic authority. */
  semanticEnvironmentKey: string
  /** From extract/columns: fingerprints of the referenced relations (their epochs). */
  schemaFingerprint: string
  /** Execute-time placeholder values, when the query is parameterized. */
  bindings?: Record<string, unknown>
}

/** Both identity keys for a query, from its rendered SQL and its environment. */
function identityOf(builder: unknown, env: IdentityEnv): Identity {
  const toSQL = (builder as { toSQL?: () => { sql: string; params: unknown[] } }).toSQL
  assertUsage(typeof toSQL === 'function', 'identityOf expects a drizzle query builder (no toSQL()).')
  const { sql, params } = toSQL.call(builder)
  const planKey = planKeyOf({
    dialect: env.dialect,
    semanticEnvironmentKey: env.semanticEnvironmentKey,
    schemaFingerprint: env.schemaFingerprint,
    sql,
  })
  return { instanceKey: instanceKeyOf(planKey, params, env.bindings) }
}

/** Structural key. `sql` from toSQL() is already parameterized (values normalized to
 *  $1/?), so it is the SQL shape with values removed. Every component is length-framed
 *  so no component can bleed into another (e.g. a space inside the SQL cannot forge a
 *  different (env, sql) pair with the same key). */
function planKeyOf(parts: {
  dialect: string
  semanticEnvironmentKey: string
  schemaFingerprint: string
  sql: string
}): string {
  return [parts.semanticEnvironmentKey, parts.dialect, parts.schemaFingerprint, parts.sql].map(frame).join('')
}

/** Graph-state key: the plan plus the canonicalized resolved bindings. Placeholders are
 *  resolved from `bindings`; an unresolved `Placeholder` (no bindings) encodes distinctly.
 *  Length-framing makes the (planKey, params) pair collision-free. */
function instanceKeyOf(planKey: string, params: unknown[], bindings?: Record<string, unknown>): string {
  const resolved = bindings ? fillPlaceholders(params, bindings) : params
  return frame(planKey) + frame(resolved.map(encodeParam).join(''))
}

function encodeParam(param: unknown): string {
  return is(param, Placeholder) ? `p${frame(param.name)}` : canonicalValue(param)
}
