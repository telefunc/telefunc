export { markErrorClass }

/** Marks `errorClass`'s prototype as Telefunc's `kind` of error and makes `instanceof errorClass` read that mark, so an
 *  error made by another copy of Telefunc (a Vite dev server loads one beside Node's) answers for this copy's class and
 *  its ancestors. `kind` is the name the class is exported as, the mark 0.3.1's `Abort` and `ShieldValidationError` carry. */
function markErrorClass(errorClass: { prototype: object }, kind: string): void {
  const mark = Symbol.for(`telefunc.${kind}`)
  Object.defineProperty(errorClass.prototype, mark, { value: true })
  Object.defineProperty(errorClass, Symbol.hasInstance, {
    value: (thing: unknown) => typeof thing === 'object' && thing !== null && mark in thing,
  })
}
