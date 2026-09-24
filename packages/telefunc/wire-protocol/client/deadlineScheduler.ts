export { DeadlineScheduler }

class DeadlineScheduler {
  private timer: ReturnType<typeof setTimeout> | null = null
  private scheduledAt = 0

  constructor(private readonly run: () => void) {}

  schedule(deadlineAt: number): void {
    if (this.timer && this.scheduledAt <= deadlineAt) return

    this.cancel()
    this.scheduledAt = deadlineAt
    this.timer = setTimeout(
      () => {
        this.timer = null
        this.scheduledAt = 0
        this.run()
      },
      Math.max(0, deadlineAt - Date.now()),
    )
  }

  cancel(): void {
    if (!this.timer) return
    clearTimeout(this.timer)
    this.timer = null
    this.scheduledAt = 0
  }
}
