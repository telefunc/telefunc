export { pingDeadline }

/** How long a wire may deliver nothing before it is taken for dead: two ping intervals. */
function pingDeadline(intervalMs: number): number {
  return intervalMs * 2
}
