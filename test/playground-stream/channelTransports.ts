export { parseChannelTransports }

/** `config.channel.transports` as PUBLIC_ENV__CHANNEL_TRANSPORTS sets it, for the page and for the runners alike:
 *  `['sse']` when it is unset or empty. */
function parseChannelTransports(value: string | undefined): Array<'sse' | 'ws'> {
  const parsed: unknown = JSON.parse(value || '["sse"]')
  if (
    !Array.isArray(parsed) ||
    parsed.length === 0 ||
    !parsed.every((transport) => transport === 'sse' || transport === 'ws')
  ) {
    throw new Error(`Invalid PUBLIC_ENV__CHANNEL_TRANSPORTS: ${value ?? '(unset)'}`)
  }
  return parsed
}
