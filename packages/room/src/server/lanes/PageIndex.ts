export { PageIndex }
export type { PageEnd }

import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import type { RoomModel } from '../../shared/RoomModel.js'
import type { LeaveCause } from '../../types.js'
import type { BinaryWants } from './binaryWants.js'
import type { DemandReport } from './DemandReport.js'
import { getOrCreate } from 'telefunc/__internal'
import { toggle, toggleEntry } from '../../utils/sets.js'
import { assert } from '../../utils/assert.js'
assertIsNotBrowser()

/** A Room end at a page: a view's stub, a participant's stub, or a server-side room's own listeners, which are their
 *  own page. */
interface PageEnd {
  /** The participants it holds. */
  heldMembers(): Iterable<string>
  /** Whether `member` is one of `heldMembers()`. */
  holds(member: string): boolean
  /** What it receives, where it receives anything. */
  readonly binaryWants?: BinaryWants
  /** The participants it holds that joined with `selfDelivery: false`. */
  selfSuppressed?(): Iterable<string>
  /** Whether `member` is one of `selfSuppressed()`. */
  suppresses?(member: string): boolean
  /** Its page's members whose own messages it doesn't get back: one set for every end of the page, kept current. */
  atPage?(suppressed: ReadonlySet<string>): void
  /** Its page's last participant of the room was removed, with `cause`. */
  pageRemoved?(cause: LeaveCause): void
}

/** A page's ends here, the participants they hold, those whose own messages none of them gets back, the token its
 *  who-entries name it by and what they were last reported with. */
type Page = {
  /** What names it here (`ServerReplacerContext.page`); none for a server-side room's own page. */
  key: string | undefined
  /** Random, never its key: who-entries leave this process. */
  token: string
  ends: Set<PageEnd>
  suppressed: Set<string>
  held: Set<string>
  /** The participants its who-entries name. */
  named: Set<string>
  /** What its ends receive, which its who-entries name. */
  scopes: Scope[]
}

/** Every track of every member, a track of every member, every track of a member, or a track of a member. */
type Scope = { member?: string; track?: string }

type PageIndexOptions = { model: RoomModel }

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
  /** The pages changed since they were reported, with the members changed of each, or `'all'`. */
  private readonly changedPages = new Map<Page, Set<string> | 'all'>()
  private readonly model: RoomModel

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
    this.reportAgain(page)
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

  /** What the end holds or receives changed, of `member` alone when given. Returns the members whose own messages its
   *  page's ends stopped or started getting back. */
  endChanged(end: PageEnd, member?: string): Set<string> {
    const page = this.pageOf.get(end)
    return page === undefined ? new Set() : this.pageChanged(page, member)
  }

  /** The member joined, or left. */
  memberChanged(member: string): void {
    for (const page of this.heldAt.get(member) ?? []) this.reportAgain(page, member)
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

  /** Reports what changed of each changed page's who-entries, a page per step. */
  *report(report: DemandReport): Generator<void> {
    for (const [page, members] of this.changedPages) {
      this.changedPages.delete(page)
      this.reportPage(page, members, report)
      yield
    }
  }

  /** The next planning pass reports the page again, of `member` alone when given. */
  private reportAgain(page: Page, member?: string): void {
    const members = this.changedPages.get(page)
    if (member === undefined) this.changedPages.set(page, 'all')
    else if (members === undefined) this.changedPages.set(page, new Set([member]))
    else if (members !== 'all') members.add(member)
  }

  private pageChanged(page: Page, member?: string): Set<string> {
    const suppressed = new Set<string>()
    this.reportAgain(page, member)
    if (member === undefined) this.readHolders(page, suppressed)
    else this.readHolder(page, member, suppressed)
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

  /** `readHolders()` for `member` alone. */
  private readHolder(page: Page, member: string, changed: Set<string>): void {
    let held = false
    let suppressed = false
    for (const end of page.ends) {
      held ||= end.holds(member)
      suppressed ||= end.suppresses?.(member) === true
    }
    toggleEntry(this.heldAt, member, page, held)
    toggle(page.held, member, held)
    if (page.suppressed.has(member) === suppressed) return
    toggle(page.suppressed, member, suppressed)
    changed.add(member)
  }

  /** Tells the page's who-entries that name `members` as they are now, or with `'all'`, every entry again. */
  private reportPage(page: Page, members: Set<string> | 'all', report: DemandReport): void {
    if (members === 'all') {
      tellWho(page, report, false)
      page.scopes = [...page.ends].flatMap((end) => [...scopesOf(end.binaryWants)])
      page.named = new Set([...page.held].filter((member) => this.names(page, member)))
      tellWho(page, report, true)
      return
    }
    const wasNobody = page.named.size === 0
    for (const member of members) {
      const named = this.names(page, member)
      if (named === page.named.has(member)) continue
      toggle(page.named, member, named)
      for (const scope of page.scopes) report.setWho({ page: page.token, by: member, ...scope }, named)
    }
    const nobody = page.named.size === 0
    if (nobody === wasNobody) return
    for (const scope of page.scopes) report.setWho({ page: page.token, ...scope }, nobody)
  }

  /** A hidden participant, and one whose join hasn't applied here yet, is never named. */
  private names(page: Page, member: string): boolean {
    return page.held.has(member) && this.model.memberHidden(member) === false
  }
}

function newPage(key: string | undefined): Page {
  const token = crypto.randomUUID()
  return { key, token, ends: new Set(), suppressed: new Set(), held: new Set(), named: new Set(), scopes: [] }
}

/** Tells, or untells, each entry the page's who-entries hold: each participant they name, or the page's token where
 *  they name none, in each scope. */
function tellWho(page: Page, report: DemandReport, on: boolean): void {
  const who = page.named.size > 0 ? [...page.named].map((by) => ({ page: page.token, by })) : [{ page: page.token }]
  for (const scope of page.scopes) for (const each of who) report.setWho({ ...each, ...scope }, on)
}

/** Each scope the wants cover. */
function* scopesOf(wants: BinaryWants | undefined): Generator<Scope> {
  if (wants === undefined) return
  if (wants.everyMember.all) yield {}
  for (const track of wants.everyMember.tracks) yield { track }
  for (const [member, { all, tracks }] of wants.members) {
    if (all) yield { member }
    for (const track of tracks) yield { member, track }
  }
}
