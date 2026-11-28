export { someFunc as onLoad, onStream }

const someFunc = async ({ name }: { name: string }) => {
  const message = 'Welcome ' + name
  return { message }
}

async function* onStream() {
  yield 'hello stream'
}
