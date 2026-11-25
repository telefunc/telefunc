export { importPostgres }

function importPostgres(): Promise<typeof import('./engine.js')> {
  return import('./engine.js')
}
