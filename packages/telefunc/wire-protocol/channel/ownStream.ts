export { OWN_STREAM, EVERY_STREAM }

/** The stream of a channel's frames that name none: its sends, its answers and the frames that end it. */
const OWN_STREAM = ''

/** The stream of a frame that must arrive after the lossy ones of every stream of its channel: a broadcast's plain
 *  publish, as its key orders both kinds. No lossy frame is of it. */
const EVERY_STREAM = '*'
