export { serializeTelefunctionResult }

import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../../utils/assert.js'
import { hasProp } from '../../../utils/hasProp.js'
import { lowercaseFirstLetter } from '../../../utils/lowercaseFirstLetter.js'
import { createStreamingReplacer } from '../../../wire-protocol/server/response/registry.js'
import { buildStreamingResponseBody } from '../../../wire-protocol/server/response/StreamingResponseBody.js'
import type { TelefuncIdentifier, TelefuncResponseBody } from '../../../shared/constants.js'

type SerializeResult = { type: 'text'; body: string } | { type: 'streaming'; body: ReadableStream<Uint8Array> }

function serializeTelefunctionResult(runContext: {
  telefunctionReturn: unknown
  telefunctionName: string
  telefuncFilePath: string
  telefunctionAborted: boolean
  onStreamComplete: () => void
  abortSignal: AbortSignal
}): SerializeResult {
  const bodyValue: TelefuncResponseBody = runContext.telefunctionAborted
    ? { ret: runContext.telefunctionReturn, abort: true }
    : { ret: runContext.telefunctionReturn }

  const { replacer, streamingValues } = createStreamingReplacer()

  let httpResponseBody: string
  try {
    httpResponseBody = stringify(bodyValue, { forbidReactElements: true, replacer })
  } catch (err: unknown) {
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

  if (streamingValues.length === 0) {
    return { type: 'text', body: httpResponseBody }
  }

  const telefuncId: TelefuncIdentifier = {
    telefunctionName: runContext.telefunctionName,
    telefuncFilePath: runContext.telefuncFilePath,
  }
  return {
    type: 'streaming',
    body: buildStreamingResponseBody(
      httpResponseBody,
      streamingValues,
      telefuncId,
      runContext.onStreamComplete,
      runContext.abortSignal,
    ),
  }
}
