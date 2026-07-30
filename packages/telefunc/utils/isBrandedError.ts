export { isBrandedError }

/** Cross-graph Error recognition without trusting inherited or trap-capable brand reads. */
function isBrandedError(value: unknown, brand: symbol): value is Error {
  if (typeof value !== 'object' || value === null) return false
  try {
    const brandDescriptor = Object.getOwnPropertyDescriptor(value, brand)
    const messageDescriptor = Object.getOwnPropertyDescriptor(value, 'message')
    return (
      brandDescriptor !== undefined &&
      'value' in brandDescriptor &&
      brandDescriptor.value === true &&
      messageDescriptor !== undefined &&
      'value' in messageDescriptor &&
      typeof messageDescriptor.value === 'string' &&
      Object.prototype.toString.call(value) === '[object Error]'
    )
  } catch {
    return false
  }
}
