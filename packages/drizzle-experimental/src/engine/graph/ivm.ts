// The one and only module that imports @tanstack/db-ivm. Every compile stage reaches the engine
// through this facade, so a single grep proves the seam: the operator set, the MultiSet/D2
// primitives, and the reader types the stages need are re-exported here and nowhere else. The
// documented engine behaviours we depend on (join retractions, retraction-correct reduce, the
// distinct multiplicity edge, concat, topK{limit,offset}, filterBy/antiJoin) are pinned by the
// contract cases in `ivm.spec.ts`, which fail loudly if an engine bump changes a shape.

export {
  D2,
  MultiSet,
  map,
  filter,
  concat,
  consolidate,
  output,
  join,
  reduce,
  distinct,
  keyBy,
  topK,
  filterBy,
  IVM_ENGINE_VERSION,
}
export type { IStreamBuilder, MultiSetArray, KeyValue, RootStreamBuilder }

import {
  D2,
  MultiSet,
  concat,
  consolidate,
  distinct,
  filter,
  filterBy,
  join,
  keyBy,
  map,
  output,
  reduce,
  topK,
} from '@tanstack/db-ivm'
import type { IStreamBuilder, KeyValue, MultiSetArray, RootStreamBuilder } from '@tanstack/db-ivm'

/** The engine golden. db-ivm is pre-1.0, so the pin is exact and this name is asserted
 *  by the contract spec against the installed package version. */
const IVM_ENGINE_VERSION = '0.1.18'
