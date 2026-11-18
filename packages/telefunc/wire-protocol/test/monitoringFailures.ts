export { MONITORING_FAILURES }

/** What a monitoring client an app reports bugs to may fail with: an error of its own, or that of a stream of its own
 *  it closed early. */
const MONITORING_FAILURES: Record<string, () => Error> = {
  'an error': () => new Error('monitoring client failed'),
  ERR_STREAM_PREMATURE_CLOSE: () =>
    Object.assign(new Error('monitoring stream closed early'), { code: 'ERR_STREAM_PREMATURE_CLOSE' }),
}
