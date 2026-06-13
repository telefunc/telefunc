export { someFunc as onLoad }

const someFunc = async ({ name }: { name: string }) => {
  const message = 'Welcome ' + name
  return { message }
}
