export { SeqQueue }

/** Entries in seq order that leave only at the ends: a slot left at the oldest end holds nothing, and the slots go once
 *  they are as many as the live ones, which keeps a drop amortised O(1) per entry. Indices count from the oldest live
 *  entry. */
class SeqQueue<T> {
  // Seqs in an array of their own, so a scan by seq reads only numbers.
  private seqs: number[] = []
  private items: (T | undefined)[] = []
  private head = 0

  get length(): number {
    return this.items.length - this.head
  }

  seqAt(i: number): number {
    return this.seqs[this.head + i]!
  }

  at(i: number): T {
    return this.items[this.head + i]!
  }

  /** The index of the oldest entry after `seq`, `length` where none is. */
  indexAfter(seq: number): number {
    const seqs = this.seqs
    let i = this.head
    while (i < seqs.length && seqs[i]! <= seq) i++
    return i - this.head
  }

  push(seq: number, item: T): void {
    this.seqs.push(seq)
    this.items.push(item)
  }

  /** Drops the `count` oldest entries. */
  dropOldest(count: number): void {
    if (count === 0) return
    const head = this.head + count
    if (head >= this.items.length) {
      this.clear()
      return
    }
    if (head >= this.items.length - head) {
      this.seqs = this.seqs.slice(head)
      this.items = this.items.slice(head)
      this.head = 0
      return
    }
    for (let i = this.head; i < head; i++) this.items[i] = undefined
    this.head = head
  }

  pop(): T {
    this.seqs.pop()
    const item = this.items.pop()!
    if (this.head >= this.items.length) this.clear()
    return item
  }

  clear(): void {
    this.seqs.length = 0
    this.items.length = 0
    this.head = 0
  }
}
