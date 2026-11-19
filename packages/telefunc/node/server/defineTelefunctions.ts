export { defineTelefunctions }

import { assertUsage } from '../../utils/assert.js'
import { isObject } from '../../utils/isObject.js'
import {
  registerTelefunction,
  loadTelefuncFilesUsingRegistration,
} from './runTelefunc/loadTelefuncFilesUsingRegistration.js'
import { getTelefunctionKey } from '../../utils/getTelefunctionKey.js'

function defineTelefunctions<T extends Record<string, (...args: any[]) => any>>(api: T): T {
  assertUsage(isObject(api), '`defineTelefunctions(api)`: argument `api` must be an object.')

  const registered = loadTelefuncFilesUsingRegistration()

  for (const [name, telefunction] of Object.entries(api)) {
    assertUsage(
      typeof telefunction === 'function',
      `\`defineTelefunctions(api)\`: expected \`api.${name}\` to be a function, but got \`${typeof telefunction}\`.`,
    )
    assertUsage(
      !name.includes(':'),
      `\`defineTelefunctions(api)\`: telefunction name \`"${name}"\` must not contain \`:\`.`,
    )
    assertUsage(
      name !== 'then',
      '`defineTelefunctions(api)`: "then" is a reserved name and cannot be used as a telefunction name.',
    )

    if (registered && registered[''] && Object.hasOwn(registered[''], name)) {
      assertUsage(false, `\`defineTelefunctions(api)\`: telefunction \`"${name}"\` is already registered.`)
    }

    const key = getTelefunctionKey('', name)
    Object.assign(telefunction, {
      _key: key,
      _isStandalone: true,
    })

    registerTelefunction(telefunction as any, name, '')
  }

  return api
}
