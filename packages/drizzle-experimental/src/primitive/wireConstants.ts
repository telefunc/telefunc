export { SERIALIZER_PREFIX_LIVE }

// The wire tag for a Live, owned by this package rather than by telefunc core's constants module.
//
// Declared in a module with NO imports so both halves can share it: the server replacer imports
// `telefunc`, the client reviver imports `telefunc/client`, and neither may pull in the other's entry —
// a browser bundle must not reach the server graph. A shared constant with a drifting value would be a
// silent wire break (the client would simply never recognise the tag), so it lives in exactly one place.
const SERIALIZER_PREFIX_LIVE = '!TelefuncLive:'
