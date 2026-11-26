export { serializeTelefunctionResult }
export type { TelefuncId }

import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../../utils/assert.js'
import { hasProp } from '../../../utils/hasProp.js'
import { lowercaseFirstLetter } from '../../../utils/lowercaseFirstLetter.js'
import { createStreamingReplacer } from '../../../wire-protocol/values/server/response/registry.js'
import { getServerExtensionTypes } from '../serverConfig.js'
import { ResponseValues, type ResponseValuesRun, type SerializeResult, type TelefuncId } from './ResponseValues.js'

function serializeTelefunctionResult(
  runContext: ResponseValuesRun & { telefunctionReturn: unknown; telefunctionAborted: boolean },
): SerializeResult {
  const bodyValue = runContext.telefunctionAborted
    ? { ret: runContext.telefunctionReturn, abort: true }
    : { ret: runContext.telefunctionReturn }
  const values = new ResponseValues(runContext)
  const replacer = createStreamingReplacer(
    (value) => values.contextFor(value),
    (replaced) => values.onReplaced(replaced),
    getServerExtensionTypes().responseTypes,
  )
  let text: string
  try {
    text = stringify(bodyValue, { forbidReactElements: true, replacer })
  } catch (err: unknown) {
    runContext.requestContext.responseAbort.abort()
    assert(hasProp(err, 'message', 'string'))
    assertUsage(
      false,
      [
        `Cannot serialize value returned by telefunction ${runContext.telefunctionName}() (${runContext.telefuncFilePath}).`,
        'Make sure that telefunctions always return a serializable value.',
        `Serialization error: ${lowercaseFirstLetter(err.message)}`,
      ].join(' '),
    )
  }
  return values.finish(text)
}
