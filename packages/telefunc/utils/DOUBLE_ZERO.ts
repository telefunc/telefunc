export { DOUBLE_ZERO }

/** 0 as V8 stores a fraction: a field that starts at it, or at an integer plus it, keeps its shape once it holds a
 *  fraction, where one that starts at an integer changes it, which deoptimizes the code that read the field. */
const DOUBLE_ZERO = -0
