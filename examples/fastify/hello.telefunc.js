export { hello }

async function hello({ name }) {
  return { message: `Welcome ${name}` }
}
