export { replaceParsedValues }

import { isObject } from '../../../../utils/isObject.js'

/** Substitutes values in the containers built by json-serializer. A substituted value is opaque. */
function replaceParsedValues(
  value: unknown,
  replacements: ReadonlyMap<object, unknown>,
  isOpaque?: (value: object) => boolean,
): unknown {
  if (!isObject(value)) return value
  if (replacements.has(value)) return replacements.get(value)
  if (isOpaque?.(value)) return value
  if (Array.isArray(value)) {
    value.forEach((item, i) => {
      value[i] = replaceParsedValues(item, replacements, isOpaque)
    })
  } else if (value instanceof Map) {
    const entries = [...value]
    value.clear()
    for (const [key, item] of entries) {
      value.set(replaceParsedValues(key, replacements, isOpaque), replaceParsedValues(item, replacements, isOpaque))
    }
  } else if (value instanceof Set) {
    const items = [...value]
    value.clear()
    for (const item of items) value.add(replaceParsedValues(item, replacements, isOpaque))
  } else {
    const prototype = Object.getPrototypeOf(value)
    if (prototype === Object.prototype || prototype === null) {
      for (const key of Object.keys(value)) value[key] = replaceParsedValues(value[key], replacements, isOpaque)
    }
  }
  return value
}
