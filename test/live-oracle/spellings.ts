export { arrayOf, CODES, REFS, SPELLED_COLUMNS, spell }
export type { Spelling, SpelledColumn }

/** The values the writes give `todos.code` (a `char(4)`) and `todos.ref` (a `uuid`). */
const CODES = ['ab', 'cd', 'abcd', 'x']
const REFS = [
  'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
  '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
  'f47ac10b-58cc-4372-a567-0e02b2c3d479',
  '00000000-0000-0000-0000-0000000000ff',
]

const SPELLED_COLUMNS = ['ref', 'code', 'priority', 'amount', 'done', 'updated_at', 'text'] as const
type SpelledColumn = (typeof SPELLED_COLUMNS)[number]

/** A value compared with a column of `todos`, spelled as an app may send it: as a parameter or inline, followed by
 *  `cast` (SQL such as `::numeric::int`). Postgres reads many spellings as one value, and some casts change it.
 *  `text`: the value it spells, in Postgres's text form. */
type Spelling = { column: SpelledColumn; text: string; value: string; cast: string; inline: boolean }

/** A spelling of `text`, a value of `column` in Postgres's text form. */
function spell(column: SpelledColumn, text: string, random: () => number): Spelling {
  const pick = <T>(items: readonly T[]): T => items[Math.floor(random() * items.length)]!
  const [value, cast] = pick(spellingsOf(column, text))
  return { column, text, value, cast, inline: random() < 0.3 }
}

/** The spellings as a Postgres array, in its text form: one parameter for `= any($1)` in every driver. An element
 *  has no cast: one that needs its cast is spelled as the value it gives. */
function arrayOf(spellings: Spelling[]): string {
  const element = ({ text, value, cast }: Spelling) => (cast ? text : value).replace(/["\\]/g, '\\$&')
  return `{${spellings.map((spelling) => `"${element(spelling)}"`).join(',')}}`
}

/** Spellings of one value, each with the cast that follows it. */
function spellingsOf(column: SpelledColumn, text: string): [string, string][] {
  switch (column) {
    case 'ref': {
      const digits = text.replace(/-/g, '')
      return [
        [text, ''],
        [text.toUpperCase(), ''],
        [digits, ''],
        [`{${text}}`, ''],
        [digits.replace(/(.{4})(?=.)/g, '$1-'), ''],
        [text.toUpperCase(), '::uuid'],
      ]
    }
    case 'code':
      return [
        [text, ''],
        [text.padEnd(4), ''],
        [`${text} `, ''],
        [text, '::char(4)'],
        [`${text}zzzz`, `::char(${text.length})`],
        [`${text}zz`, `::varchar(${text.length})`],
      ]
    case 'priority':
      return [
        [text, ''],
        [` ${text} `, ''],
        [`+${text}`, ''],
        [`0${text}`, ''],
        [`${text}.4`, '::numeric::int'],
        [Number(text) > 0 ? `${Number(text) - 1}.6` : '-0.4', '::numeric::int'],
      ]
    case 'amount':
      return [
        [text, ''],
        [`${text}0`, ''],
        [`+${text}`, ''],
        [` ${text} `, ''],
        [`${text}9`, '::numeric(12, 2)'],
        [text, '::numeric(12, 1)'],
      ]
    case 'done':
      return text === 'true'
        ? [
            ['t', ''],
            ['TRUE', ''],
            ['yes', ''],
            ['on', ''],
            ['1', ''],
            ['y', '::boolean'],
          ]
        : [
            ['f', ''],
            ['FALSE', ''],
            ['no', ''],
            ['off', ''],
            ['0', ''],
            ['n', '::boolean'],
          ]
    case 'updated_at': {
      // `text` is in UTC: `2026-11-10 08:05:00+00`.
      const at = new Date(`${text.replace(' ', 'T').slice(0, -3)}Z`)
      const shifted = (hours: number) => {
        const local = new Date(at.getTime() + hours * 3_600_000).toISOString().slice(0, 23)
        return `${local}${hours < 0 ? '-' : '+'}${String(Math.abs(hours)).padStart(2, '0')}:00`
      }
      return [
        [text, ''],
        [at.toISOString(), ''],
        [shifted(2), ''],
        [shifted(-5), ''],
        [shifted(9), '::timestamptz'],
      ]
    }
    case 'text':
      return [
        [text, ''],
        [`${text}xyz`, `::varchar(${text.length})`],
        [text, `::char(${text.length + 2})`],
        [`${text}  `, `::char(${text.length})`],
      ]
  }
}
