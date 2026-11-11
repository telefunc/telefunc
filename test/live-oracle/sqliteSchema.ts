export { sqliteSeed }

import { USERS } from './schema.ts'

/** The SQLite lane's tables: the Postgres lane's main ones, in SQLite's types, with what SQLite compares its own way.
 *  `users.nick` ignores ASCII case (`NOCASE`), `todos.due` holds `NULL`s, which SQLite sorts first. `settings` is
 *  written with `REPLACE`, which deletes the row it conflicts with. */
const SCHEMA = `
pragma foreign_keys = on;
create table users (id integer primary key, name text not null, team int not null, nick text collate nocase);
create table todos (
  id integer primary key,
  user_id int not null references users (id) on delete cascade,
  text text not null,
  done integer not null default 0,
  priority int not null default 0,
  amount real not null default 0,
  updated_at text not null default '2026-11-10T08:00:00.000Z',
  due int
);
create table tags (id integer primary key, todo_id int not null references todos (id) on delete cascade, name text not null);
-- Written by a trigger, also when a user's deletion cascades to their todos.
create table deleted_todos (id integer primary key, user_id int not null, todo_id int not null);
create trigger todos_deleted after delete on todos begin
  insert into deleted_todos (user_id, todo_id) values (old.user_id, old.id);
end;
create table settings (id integer primary key, name text not null unique, value int not null);
`

const NICKS = ['ann', 'Ann', 'BOB', 'bob', 'Cy', null]

function sqliteSeed(random: () => number): string {
  const users = Array.from({ length: USERS }, (_, i) => {
    const nick = NICKS[i % NICKS.length]
    return `('user ${i + 1}', ${(i % 3) + 1}, ${nick === null ? 'null' : `'${nick}'`})`
  })
  const todos = Array.from({ length: 40 }, (_, i) => {
    const user = 1 + Math.floor(random() * USERS)
    // Mixed case and a backslash: SQLite's `LIKE` ignores ASCII case and has no escape character.
    const text = `${['a', 'A', 'b', 'a\\', 'B'][i % 5]}${i}`
    const at = new Date(Date.UTC(2026, 10, 10, 8, 0, Math.floor(random() * 1000))).toISOString()
    const due = random() < 0.3 ? 'null' : Math.floor(random() * 10)
    return `(${user}, '${text}', ${random() < 0.4 ? 1 : 0}, ${Math.floor(random() * 5)}, ${(random() * 100).toFixed(2)}, '${at}', ${due})`
  })
  const tags = Array.from({ length: 20 }, (_, i) => `(${1 + Math.floor(random() * 40)}, 'tag ${i % 4}')`)
  const settings = Array.from({ length: 4 }, (_, i) => `('s${i}', ${i})`)
  return `${SCHEMA}
insert into users (name, team, nick) values ${users.join(', ')};
insert into todos (user_id, text, done, priority, amount, updated_at, due) values ${todos.join(', ')};
insert into tags (todo_id, name) values ${tags.join(', ')};
insert into settings (name, value) values ${settings.join(', ')};`
}
