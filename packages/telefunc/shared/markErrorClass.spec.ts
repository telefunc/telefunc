import { expect, test, vi } from 'vitest'

// Every entry an app imports Telefunc's error classes from, as one copy of Telefunc.
const loadCopy = async () =>
  Object.assign(
    {},
    await import('../node/server/index.js'),
    await import('../client/index.js'),
    await import('../node/client.js'),
  )
function errorClasses(copy: object) {
  const classes = new Map<string, Function>()
  for (const [name, value] of Object.entries(copy))
    if (typeof value === 'function' && Error.prototype.isPrototypeOf(value.prototype)) classes.set(name, value)
  return classes
}

// A second copy beside it, as a Vite dev server loads one beside the one Node loads: an app's telefunction imports its
// error classes from one copy while the errors it catches were made by the other. Loaded while the file is collected.
const thisCopy = errorClasses(await loadCopy())
vi.resetModules()
const otherCopy = errorClasses(await loadCopy())

// `instanceof` reads only what an object inherits, so an object made on a class's prototype stands for its instance.
const instanceOf = (errorClass: Function) => Object.create(errorClass.prototype)
const descends = (errorClass: Function, from: Function) => from.prototype.isPrototypeOf(errorClass.prototype)

test('the walk finds the exported error classes, each loaded twice', () => {
  expect([...thisCopy.keys()]).toEqual([...otherCopy.keys()])
  expect(thisCopy.size).toBeGreaterThan(1)
  for (const [name, errorClass] of thisCopy) expect(otherCopy.get(name), name).not.toBe(errorClass)
})

test.for([...thisCopy.keys()])('%s carries its mark, and holds under `instanceof` across two copies', (name) => {
  const errorClass = thisCopy.get(name)
  const other = otherCopy.get(name)
  if (!errorClass || !other) throw new Error(`${name} isn't in both copies`)
  expect(instanceOf(other)).toBeInstanceOf(errorClass)
  expect(instanceOf(errorClass)).toBeInstanceOf(other)
  const ofSubclass = Object.create(instanceOf(other))
  for (const ancestor of thisCopy.values())
    if (ancestor === errorClass || descends(errorClass, ancestor)) expect(ofSubclass).toBeInstanceOf(ancestor)

  expect(Object.getOwnPropertySymbols(errorClass.prototype)).toContain(Symbol.for(`telefunc.${name}`))
})

test.for(['Abort', 'ShieldValidationError'])("%s takes 0.3.1's errors, which carry their mark themselves", (name) => {
  const errorClass = thisCopy.get(name)
  if (!errorClass) throw new Error(`${name} isn't exported`)
  // As 0.3.1 made them: an instance of a class with no mark on its prototype, with the mark as its own field.
  class ErrorOf031 extends Error {}
  expect(Object.assign(new ErrorOf031(), { [Symbol.for(`telefunc.${name}`)]: true })).toBeInstanceOf(errorClass)
  expect(new ErrorOf031()).not.toBeInstanceOf(errorClass)
})

test.for([...thisCopy.keys()])("%s takes no other class's error, nor what only looks like one", (name) => {
  const errorClass = thisCopy.get(name)
  if (!errorClass) throw new Error(`${name} isn't exported`)
  for (const [otherName, other] of otherCopy)
    if (otherName !== name && !descends(thisCopy.get(otherName) ?? other, errorClass))
      expect(instanceOf(other), otherName).not.toBeInstanceOf(errorClass)
  expect(Object.assign(new Error(), { name })).not.toBeInstanceOf(errorClass)
  for (const thing of [null, undefined, name, 0]) expect(thing).not.toBeInstanceOf(errorClass)
})
