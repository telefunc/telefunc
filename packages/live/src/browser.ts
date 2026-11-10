export { withLiveQueries }

// A `.telefunc.ts` file's imports are scanned for the browser too; the server code never runs there.
function withLiveQueries<T>(database: T): T {
  return database
}
