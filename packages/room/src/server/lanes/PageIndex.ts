export { PageIndex }
export type { PageEnd }

import { getOrCreate } from 'telefunc/__internal'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import type { RoomModel } from '../../shared/RoomModel.js'
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
}

/** A page's ends here, the token its who-entries name it by, and what it was last reported with. */
type Page = { token: string; ends: Set<PageEnd>; held: Set<string>; told: WhoEntry[] }

type PageIndexOptions = { model: Pick<RoomModel, 'memberHidden'> }

/** The pages holding this room here, so a watch is told as that of the people at its page: the participants its ends
 *  hold, or the page's token where it holds none it may name. A page's ends are the ones that attached last on one
 *  connection; a server-side room is its own page. A page's entries are reported in the planning pass after it
 *  changed, never as it changes. */
class PageIndex {
  private readonly pages = new Map<object, Page>()
  private readonly pageOf = new Map<PageEnd, object>()
  /** The pages each member was held at when they were last reported, which its join and leave report again. */
  private readonly heldAt = new Map<string, Set<object>>()
  private readonly changedPages = new Set<object>()
  private readonly model: PageIndexOptions['model']

  constructor({ model }: PageIndexOptions) {
    this.model = model
  }

  /** Whether some page changed since it was reported. */
  get changed(): boolean {
    return this.changedPages.size > 0
  }

  /** The end is at `page` from now on; at the same page again, nothing changes. */
  place(end: PageEnd, page: object): void {
    const was = this.pageOf.get(end)
    if (was === page) return
    if (was !== undefined) this.remove(end)
    this.pageOf.set(end, page)
    getOrCreate(this.pages, page, newPage).ends.add(end)
    this.changedPages.add(page)
  }

  remove(end: PageEnd): void {
    const page = this.pageOf.get(end)
    if (page === undefined) return
    this.pageOf.delete(end)
    this.pages.get(page)!.ends.delete(end)
    this.changedPages.add(page)
  }

  /** What the end holds or receives changed. */
  endChanged(end: PageEnd): void {
    const page = this.pageOf.get(end)
    if (page !== undefined) this.changedPages.add(page)
  }

  /** The member joined, or left. */
  memberChanged(member: string): void {
    for (const page of this.heldAt.get(member) ?? []) this.changedPages.add(page)
  }

  /** Reports each changed page's who-entries again, a page per step. */
  *report(report: Pick<DemandReport, 'setWho'>): Generator<void> {
    for (const page of this.changedPages) {
      this.changedPages.delete(page)
      this.reportPage(page, report)
      yield
    }
  }

  private reportPage(key: object, report: Pick<DemandReport, 'setWho'>): void {
    const page = this.pages.get(key)
    if (page === undefined) return
    for (const entry of page.told) report.setWho(entry, false)
    const held = new Set<string>()
    for (const end of page.ends) for (const member of end.heldMembers()) held.add(member)
    for (const member of page.held) if (!held.has(member)) toggleEntry(this.heldAt, member, key, false)
    for (const member of held) toggleEntry(this.heldAt, member, key, true)
    page.held = held
    // A hidden participant, and one whose join hasn't applied here yet, is never named.
    const named = [...held].filter((member) => this.model.memberHidden(member) === false)
    const who = named.length > 0 ? named.map((by) => ({ page: page.token, by })) : [{ page: page.token }]
    page.told = []
    for (const end of page.ends) {
      for (const scope of scopesOf(end.binaryWants)) {
        for (const each of who) page.told.push({ ...each, ...scope })
      }
    }
    for (const entry of page.told) report.setWho(entry, true)
    if (page.ends.size === 0) this.pages.delete(key)
  }
}

function newPage(): Page {
  return { token: crypto.randomUUID(), ends: new Set(), held: new Set(), told: [] }
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
