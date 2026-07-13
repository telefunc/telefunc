export { onCountdown }

// AsyncGenerator: streams values to the client over time, see https://telefunc.com/stream
async function* onCountdown(from) {
  for (let i = from; i >= 0; i--) {
    yield i
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }
}
