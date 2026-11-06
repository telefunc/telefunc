export { PublishBody }

/** A publish's frame, which every subscriber on this server holds and sends behind a head of its own. */
class PublishBody {
  readonly frame: Uint8Array<ArrayBuffer>

  constructor(frame: Uint8Array<ArrayBuffer>) {
    this.frame = frame
  }
}
