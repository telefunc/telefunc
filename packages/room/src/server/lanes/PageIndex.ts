export { PageIndex }
export type { PageEnd }

import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import type { RoomModel } from '../../shared/RoomModel.js'
import type { LeaveCause } from '../../types.js'
import type { WhoEntry } from '../protocol.js'
import type { BinaryWants } from './binaryWants.js'
import type { DemandReport } from './DemandReport.js'
import { getOrCreate } from 'telefunc/__internal'
import { toggleEntry } from '../../utils/sets.js'
import { assert } from '../../utils/assert.js'
assertIsNotBrowser()

/** A Room end at a page: a view's stub, a participant's stub, or a server-side room's own listeners, which are their
 *  own page. */
interface PageEnd {
  /** The participants it holds. */
  heldMembers(): Iterable<string>
  /** What it receives, where it receives anything. */
  readonly binaryWants?: BinaryWants
  /** The participants it holds that joined with `selfDelivery: false`. */
  selfSuppressed?(): Iterable<string>
  /** Its page's members whose own messages it doesn't get back: one set for every end of the page, kept current. */
  atPage?(suppressed: ReadonlySet<string>): void
  /** Its page's last participant of the room was removed, with `cause`. */
  pageRemoved?(cause: LeaveCause): void
}

/** A page's ends here, the participants they hold, those whose own messages none of them gets back, the token its
 *  who-entries name it by and what it was last reported with. */
type Page = {
  /** What names it here (`ServerReplacerContext.page`); none for a server-side room's own page. */
  key: string | undefined
  /** Random, never its key: who-entries leave this process. */
  token: string
  ends: Set<PageEnd>
  suppressed: Set<string>
  held: Set<string>
  told: WhoEntry[]
}

type PageIndexOptions = { model: Pick<RoomModel, 'memberHidden'> }

/** The pages holding this room here, which decide whose own messages a page gets back, which pages a removal closes,
 *  and whom a watch is told as.
 *  An end is at the page of the response that returned it, from then on: the page its client's session names, whatever
 *  telefunction or connection the end came by. A server-side room is its own page.
 *  A watch is told as that of the participants the page's ends hold, or of the page's token where they hold none it
 *  may name, in the planning pass after the page changed, never as it changes. */
class PageIndex {
  private readonly pageOf = new Map<PageEnd, Page>()
  private readonly pageOfKey = new Map<string, Page>()
  /** The pages holding each member, which its join and leave report again. */
  private readonly heldAt = new Map<string, Set<Page>>()
  private readonly changedPages = new Set<Page>()
  private readonly model: PageIndexOptions['model']

  constructor({ model }: PageIndexOptions) {
    this.model = model
  }

  /** Whether some page changed since it was reported. */
  get changed(): boolean {
    return this.changedPages.size > 0
  }

  /** The end is at the page `key` names, or at a page of its own without one, until it is removed. Returns the members
   *  whose own messages some end stopped or started getting back. */
  place(end: PageEnd, key?: string): Set<string> {
    const suppressed = new Set<string>()
    const was = this.pageOf.get(end)
    if (was !== undefined) {
      assert(was.key === key)
      return suppressed
    }
    const page = key === undefined ? newPage(key) : getOrCreate(this.pageOfKey, key, () => newPage(key))
    this.pageOf.set(end, page)
    page.ends.add(end)
    end.atPage?.(page.suppressed)
    for (const member of page.suppressed) suppressed.add(member)
    this.changedPages.add(page)
    this.readHolders(page, suppressed)
    return suppressed
  }

  /** Returns the members whose own messages the page's other ends get back from now on. */
  remove(end: PageEnd): Set<string> {
    const page = this.pageOf.get(end)
    if (page === undefined) return new Set()
    this.pageOf.delete(end)
    page.ends.delete(end)
    if (page.ends.size === 0 && page.key !== undefined) this.pageOfKey.delete(page.key)
    return this.pageChanged(page)
  }

  /** What the end holds or receives changed. Returns the members whose own messages its page's ends stopped or
   *  started getting back. */
  endChanged(end: PageEnd): Set<string> {
    const page = this.pageOf.get(end)
    return page === undefined ? new Set() : this.pageChanged(page)
  }

  /** The member joined, or left. */
  memberChanged(member: string): void {
    for (const page of this.heldAt.get(member) ?? []) this.changedPages.add(page)
  }

  /** The pages holding the member now. */
  pagesHolding(member: string): Page[] {
    return [...(this.heldAt.get(member) ?? [])]
  }

  /** Closes the room's views at each of `pages` holding no participant now: the last one it held was removed, with
   *  `cause`. */
  closeRemoved(pages: Page[], cause: LeaveCause): void {
    for (const page of pages) {
      if (page.held.size > 0) continue
      for (const end of [...page.ends]) end.pageRemoved?.(cause)
    }
  }

  /** Reports each changed page's who-entries again, a page per step. */
  *report(report: Pick<DemandReport, 'setWho'>): Generator<void> {
    for (const page of this.changedPages) {
      this.changedPages.delete(page)
      this.reportPage(page, report)
      yield
    }
  }

  private pageChanged(page: Page): Set<string> {
    const suppressed = new Set<string>()
    this.changedPages.add(page)
    this.readHolders(page, suppressed)
    return suppressed
  }

  /** Reads whom the page's ends hold, and keeps its set the members they hold with `selfDelivery: false`, adding each
   *  member that set gains or loses to `changed`. */
  private readHolders(page: Page, changed: Set<string>): void {
    const held = new Set<string>()
    const suppressed = new Set<string>()
    for (const end of page.ends) {
      for (const member of end.heldMembers()) held.add(member)
      for (const member of end.selfSuppressed?.() ?? []) suppressed.add(member)
    }
    for (const member of page.held) if (!held.has(member)) toggleEntry(this.heldAt, member, page, false)
    for (const member of held) toggleEntry(this.heldAt, member, page, true)
    page.held = held
    for (const member of page.suppressed) {
      if (suppressed.has(member)) continue
      page.suppressed.delete(member)
      changed.add(member)
    }
    for (const member of suppressed) {
      if (page.suppressed.has(member)) continue
      page.suppressed.add(member)
      changed.add(member)
    }
  }

  private reportPage(page: Page, report: Pick<DemandReport, 'setWho'>): void {
    for (const entry of page.told) report.setWho(entry, false)
    // A hidden participant, and one whose join hasn't applied here yet, is never named.
    const named = [...page.held].filter((member) => this.model.memberHidden(member) === false)
    const who = named.length > 0 ? named.map((by) => ({ page: page.token, by })) : [{ page: page.token }]
    page.told = []
    for (const end of page.ends) {
      for (const scope of scopesOf(end.binaryWants)) {
        for (const each of who) page.told.push({ ...each, ...scope })
      }
    }
    for (const entry of page.told) report.setWho(entry, true)
  }
}

function newPage(key: string | undefined): Page {
  return { key, token: crypto.randomUUID(), ends: new Set(), suppressed: new Set(), held: new Set(), told: [] }
}

/** Each scope the wants cover: every track of every member, a track of every member, every track of a member, or a
 *  track of a member. */
function* scopesOf(wants: BinaryWants | undefined): Generator<{ member?: string; track?: string }> {
  if (wants === undefined) return
  if (wants.everyMember.all) yield {}
  for (const track of wants.everyMember.tracks) yield { track }
  for (const [member, { all, tracks }] of wants.members) {
    if (all) yield { member }
    for (const track of tracks) yield { member, track }
  }
}
