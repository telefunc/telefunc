export { derived, liveDrizzle, liveKysely, livePrisma }

// The server entries in the browser: a `.telefunc.ts` file's imports are scanned for the browser too, and its database
// module with them, but none of it runs there.

function derived<R>(compute: () => R): { readonly data: R } {
  return { data: compute() }
}

function liveDrizzle<T>(db: T): T {
  return db
}

function liveKysely<T>(db: T): T {
  return db
}

function livePrisma<T>(db: T): T {
  return db
}
