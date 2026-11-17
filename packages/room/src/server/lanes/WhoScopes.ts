export { WhoScopes }

import { getOrCreate } from 'telefunc/__internal'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

/** The who-entries of a scope, each by its page and participant, with the reporters that told it. */
type Scope<Who> = Map<string, Who>

/** Who-entries by the scope they cover: a track of an owned member, every track of an owned member, a track of every
 *  member, or every track of every member (`undefined` for every one). */
class WhoScopes<Who> {
  private readonly pairs = new Map<string, Map<string, Scope<Who>>>()
  private readonly members = new Map<string, Scope<Who>>()
  private readonly tracks = new Map<string, Scope<Who>>()
  private readonly all: Scope<Who> = new Map()

  /** The scope's entries, made where it has none. */
  of(member: string | undefined, track: string | undefined): Scope<Who> {
    if (member === undefined) return track === undefined ? this.all : getOrCreate(this.tracks, track, () => new Map())
    if (track === undefined) return getOrCreate(this.members, member, () => new Map())
    return getOrCreate(
      getOrCreate(this.pairs, member, () => new Map()),
      track,
      () => new Map(),
    )
  }

  /** The scopes that cover `track` of `member`; the every-track ones only where `everyTrack`. */
  covering(member: string, track: string, everyTrack: boolean): Array<Scope<Who> | undefined> {
    const scopes = [this.pairs.get(member)?.get(track), this.tracks.get(track)]
    if (everyTrack) scopes.push(this.members.get(member), this.all)
    return scopes
  }

  /** Each scope, with what it covers. */
  *scopes(): Generator<[member: string | undefined, track: string | undefined, scope: Scope<Who>]> {
    for (const [member, byTrack] of this.pairs) for (const [track, scope] of byTrack) yield [member, track, scope]
    for (const [member, scope] of this.members) yield [member, undefined, scope]
    for (const [track, scope] of this.tracks) yield [undefined, track, scope]
    yield [undefined, undefined, this.all]
  }

  /** Forgets the scope once it holds no entry. */
  prune(member: string | undefined, track: string | undefined): void {
    if (member === undefined) {
      if (track !== undefined && this.tracks.get(track)?.size === 0) this.tracks.delete(track)
    } else if (track === undefined) {
      if (this.members.get(member)?.size === 0) this.members.delete(member)
    } else {
      const byTrack = this.pairs.get(member)
      if (byTrack?.get(track)?.size === 0) byTrack.delete(track)
      if (byTrack?.size === 0) this.pairs.delete(member)
    }
  }

  /** The member left: the scopes naming it go. */
  forget(member: string): void {
    this.pairs.delete(member)
    this.members.delete(member)
  }

  clear(): void {
    this.pairs.clear()
    this.members.clear()
    this.tracks.clear()
    this.all.clear()
  }
}
