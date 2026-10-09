export { Fifo }

/** A FIFO with O(1) amortized reads, where `Array#shift` would copy what's left on every read. */
class Fifo<T> {
  private items: (T | undefined)[] = []
  private head = 0

  get length(): number {
    return this.items.length - this.head
  }

  push(item: T): void {
    this.items.push(item)
  }

  shift(): T | undefined {
    if (this.head === this.items.length) return undefined
    const item = this.items[this.head] as T
    this.items[this.head++] = undefined
    if (this.head > 16 && this.head >= this.items.length >>> 1) {
      this.items = this.items.slice(this.head)
      this.head = 0
    }
    return item
  }
}
