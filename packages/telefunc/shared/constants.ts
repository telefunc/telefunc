export const STATUS_CODE_SUCCESS = 200 // "OK"
export const STATUS_CODE_MALFORMED_REQUEST = 400 // "Bad Request"
export const STATUS_BODY_MALFORMED_REQUEST = 'Malformed Telefunc Request'
export const STATUS_CODE_THROW_ABORT = 403 // "Forbidden"
export const STATUS_CODE_SHIELD_VALIDATION_ERROR = 422 // "Unprocessable Content"
export const STATUS_BODY_SHIELD_VALIDATION_ERROR = 'Shield Validation Error'
export const STATUS_CODE_INTERNAL_SERVER_ERROR = 500 // "Internal Server Error"
export const STATUS_BODY_INTERNAL_SERVER_ERROR = 'Internal Server Error'

/** Identifies a telefunction. Shared across JSON and streaming paths, server and client. */
export type TelefuncIdentifier = {
  telefunctionName: string
  telefuncFilePath: string
}

// ===== Response body (JSON path + streaming metadata) =====

/** Successful telefunction return value. */
export type TelefuncResponseBodySuccess = {
  ret: unknown
}

/** Aborted telefunction return value. */
export type TelefuncResponseBodyAbort = {
  ret: unknown
  abort: true
}

/** Wire format of the JSON response body / streaming metadata. */
export type TelefuncResponseBody = TelefuncResponseBodySuccess | TelefuncResponseBodyAbort
