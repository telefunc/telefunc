// Workaround: Chrome 158 runs a `FinalizationRegistry`'s callbacks of a navigated iframe's old realm, where its parent
// kept a reference to it, against the new page: the cleanup cleared the new page's timer. Remove once Chrome drops the
// old realm's callbacks.
export { isNavigatedAwayRealm }

/** Whether this realm's document was navigated away from (an iframe's old page): its finalizers must do nothing. */
function isNavigatedAwayRealm(): boolean {
  return typeof document !== 'undefined' && document.defaultView === null
}
