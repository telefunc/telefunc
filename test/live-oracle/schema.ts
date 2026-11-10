export { SCHEMA, seed, USERS }

import { CODES, REFS } from './spellings.ts'

const USERS = 6

/** The same tables for every ORM; each run starts from them. */
const SCHEMA = `
drop table if exists tags, todos, users, deleted_todos cascade;
drop function if exists log_deleted_todo;
create table users (id serial primary key, name text not null, team int not null);
create table todos (
  id serial primary key,
  user_id int not null references users (id) on delete cascade,
  text text not null,
  done boolean not null default false,
  priority int not null default 0,
  amount numeric(12, 2) not null default 0,
  updated_at timestamptz not null default now(),
  ref uuid not null default '${REFS[0]}',
  code char(4) not null default '${CODES[0]}'
);
create table tags (id serial primary key, todo_id int not null references todos (id) on delete cascade, name text not null);
-- Written by a trigger, also when a user's deletion cascades to their todos.
create table deleted_todos (id serial primary key, user_id int not null, todo_id int not null);
create function log_deleted_todo() returns trigger language plpgsql as $$
begin
  insert into deleted_todos (user_id, todo_id) values (old.user_id, old.id);
  return old;
end $$;
create trigger todos_deleted after delete on todos for each row execute function log_deleted_todo();
`

function seed(random: () => number): string {
  const users = Array.from({ length: USERS }, (_, i) => `('user ${i + 1}', ${(i % 3) + 1})`)
  const todos = Array.from({ length: 40 }, (_, i) => {
    const user = 1 + Math.floor(random() * USERS)
    const text = `${'abc'[i % 3]}${i}`
    const at = `'2026-11-10 08:00:00+00'::timestamptz + interval '${Math.floor(random() * 1000)} seconds'`
    const ref = REFS[Math.floor(random() * REFS.length)]
    const code = CODES[Math.floor(random() * CODES.length)]
    return `(${user}, '${text}', ${random() < 0.4}, ${Math.floor(random() * 5)}, ${(random() * 100).toFixed(2)}, ${at}, '${ref}', '${code}')`
  })
  const tags = Array.from({ length: 20 }, (_, i) => `(${1 + Math.floor(random() * 40)}, 'tag ${i % 4}')`)
  return `${SCHEMA}
insert into users (name, team) values ${users.join(', ')};
insert into todos (user_id, text, done, priority, amount, updated_at, ref, code) values ${todos.join(', ')};
insert into tags (todo_id, name) values ${tags.join(', ')};`
}
