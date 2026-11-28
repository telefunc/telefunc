export { SeqQueue }

/** Entries in seq order that leave at the ends, or from among the newest: a slot left at the oldest end holds nothing,
 *  and the slots go once they are as many as the live ones, which keeps a drop amortised O(1) per entry. Indices count
 *  from the oldest live entry. An entry is never `undefined`, which marks a slot left. */
class SeqQueue<T extends {}> {
  // Seqs in an array of their own, so a scan by seq reads only numbers.
  private seqs: number[] = []
  private items: (T | undefined)[] = []
  private head = 0

  get length(): number {
    return this.items.length - this.head
  }

  seqAt(i: number): number {
    const seq = this.seqs[this.head + i]
    if (seq === undefined) throw outOfRange(i, this.length)
    return seq
  }

  at(i: number): T {
    const item = this.items[this.head + i]
    if (item === undefined) throw outOfRange(i, this.length)
    return item
  }

  /** The index of the oldest entry after `seq`, `length` where none is. */
  indexAfter(seq: number): number {
    const seqs = this.seqs
    let i = this.head
    for (let each = seqs[i]; each !== undefined && each <= seq; each = seqs[++i]);
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
    const item = this.items.pop()
    if (item === undefined) throw new RangeError('The queue holds no entry to take')
    this.seqs.pop()
    if (this.head >= this.items.length) this.clear()
    return item
  }

  /** Takes out the entry at `i`. */
  removeAt(i: number): T {
    const item = this.at(i)
    this.seqs.splice(this.head + i, 1)
    this.items.splice(this.head + i, 1)
    if (this.head >= this.items.length) this.clear()
    return item
  }

  clear(): void {
    this.seqs.length = 0
    this.items.length = 0
    this.head = 0
  }
}

/** A read past the queue's live entries: a bug of its caller. */
function outOfRange(i: number, length: number): Error {
  return new RangeError(`The queue has no entry ${i}: it holds ${length}`)
}
