// Package surface for the IR + extraction layer of this ticket. The runtime entry
// (reactiveDrizzle, reactiveControls) lands in a later ticket and supersedes this.

export type * from './ir/types.js'
export { eval3, rowView, compareValues } from './ir/eval.js'

export { extractQueryShape } from './extract/queryShape.js'
export { extractPredicate, parsePredicate, toNNF, conjunctsOf } from './extract/predicate.js'
export type { PredicateResult } from './extract/predicate.js'
export {
  tableRefOf,
  colRefOf,
  realTableNameOf,
  primaryKeyOf,
  tableFingerprint,
  schemaFingerprint,
  demandedColumns,
} from './extract/columns.js'
export { identityOf, planKeyOf, instanceKeyOf, canonicalValue, COMPILER_ABI } from './extract/identity.js'
export type { Identity, IdentityEnv } from './extract/identity.js'

export { dialectOf, driverOf, clientOf, semanticEnvironmentKeyOf, rlsEnabledOf } from './binding/database.js'
export { selectConfigOf, usedTablesOf, isPartialSelect } from './binding/drizzleShape.js'
export type { DrizzleSelect } from './binding/drizzleShape.js'
