export { PageIndex }
export type { PageEnd }

import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import type { RoomModel } from '../../shared/RoomModel.js'
import type { LeaveCause } from '../../types.js'
import type { WhoEntry } from '../protocol.js'
import type { BinaryWants } from './binaryWants.js'
import type { DemandReport } from './DemandReport.js'
import { toggleEntry } from '../../utils/sets.js'
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
 *  who-entries name it by and what it was last reported with. A page joined into another names that one (`into`). */
type Page = {
  token: string
  ends: Set<PageEnd>
  suppressed: Set<string>
  held: Set<string>
  told: WhoEntry[]
  into: Page | null
}

type PageIndexOptions = { model: Pick<RoomModel, 'memberHidden'> }

/** The pages holding this room here, which decide whose own messages a page gets back, which pages a removal closes,
 *  and whom a watch is told as.
 *  A key names a page: the response that carried an end, until the end attaches, then each connection an end attached
 *  on. Two keys that ever named one end name one page from then on, so a page's ends returned by separate telefunctions
 *  are one page once they attached, and a reconnect or an upgrade keeps the page. A server-side room is its own page.
 *  A watch is told as that of the participants the page's ends hold, or of the page's token where they hold none it
 *  may name, in the planning pass after the page changed, never as it changes. */
class PageIndex {
  private readonly pageOf = new Map<PageEnd, Page>()
  private readonly pageOfKey = new WeakMap<object, Page>()
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

  /** The end is at the page `key` names from now on; a key that named another page joins the two. Returns the members
   *  whose own messages some end stopped or started getting back. */
  place(end: PageEnd, key: object): Set<string> {
    const suppressed = new Set<string>()
    const named = this.pageOfKey.get(key)
    const was = this.pageOf.get(end)
    let page: Page
    if (was === undefined) page = this.add(end, named === undefined ? newPage() : rootOf(named), suppressed)
    else if (named === undefined) page = was
    else page = this.join(rootOf(named), was, suppressed)
    this.pageOfKey.set(key, page)
    return suppressed
  }

  /** Returns the members whose own messages the page's other ends get back from now on. */
  remove(end: PageEnd): Set<string> {
    const page = this.pageOf.get(end)
    if (page === undefined) return new Set()
    this.pageOf.delete(end)
    page.ends.delete(end)
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

  /** The end's first page. */
  private add(end: PageEnd, page: Page, suppressed: Set<string>): Page {
    this.pageOf.set(end, page)
    page.ends.add(end)
    end.atPage?.(page.suppressed)
    for (const member of page.suppressed) suppressed.add(member)
    this.changedPages.add(page)
    this.readHolders(page, suppressed)
    return page
  }

  private pageChanged(page: Page): Set<string> {
    const suppressed = new Set<string>()
    this.changedPages.add(page)
    this.readHolders(page, suppressed)
    return suppressed
  }

  /** The pages are one from now on: the one with fewer ends moves into the other, whose token stays. */
  private join(a: Page, b: Page, suppressed: Set<string>): Page {
    if (a === b) return a
    const [page, joined] = a.ends.size >= b.ends.size ? [a, b] : [b, a]
    // Each end of either page drops, or gets back, what the other page dropped.
    for (const member of [...page.suppressed, ...joined.suppressed]) suppressed.add(member)
    for (const end of joined.ends) {
      this.pageOf.set(end, page)
      page.ends.add(end)
      end.atPage?.(page.suppressed)
    }
    joined.ends.clear()
    joined.into = page
    // The joined page's report retracts its entries.
    this.changedPages.add(joined)
    this.changedPages.add(page)
    this.readHolders(joined, suppressed)
    this.readHolders(page, suppressed)
    return page
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

function newPage(): Page {
  return { token: crypto.randomUUID(), ends: new Set(), suppressed: new Set(), held: new Set(), told: [], into: null }
}

/** The page a page was joined into, if it was. */
function rootOf(page: Page): Page {
  while (page.into !== null) page = page.into
  return page
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
