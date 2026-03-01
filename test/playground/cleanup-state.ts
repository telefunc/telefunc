export { cleanupState, resetCleanupState }

type CleanupState = Record<string, string>

// Use globalThis to ensure a single shared instance across Vite's
// separate module graphs (server vs telefunc SSR modules).
const key = '__telefunc_test_cleanupState'
const cleanupState: CleanupState = ((globalThis as any)[key] ??= {})

function resetCleanupState() {
  for (const k of Object.keys(cleanupState)) {
    delete cleanupState[k]
  }
}
