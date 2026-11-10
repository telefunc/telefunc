export { SCHEMA, seed, USERS }

import { CODES, REFS } from './spellings.ts'

const USERS = 6

/** The same tables for every ORM; each run starts from them. */
const SCHEMA = `
drop table if exists tags, todos, users, deleted_todos, notes, tag_log, note_log cascade;
drop schema if exists tenant cascade;
drop function if exists log_deleted_todo, log_tag, tag_added, log_note, note_bumped;
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
-- One name in two schemas: which table an unqualified \`notes\` means depends on the session's search_path.
create schema tenant;
create table notes (id int primary key, n int not null default 0);
create table tenant.notes (id int primary key, n int not null default 0);
insert into notes (id) select generate_series(1, 5);
insert into tenant.notes (id) select generate_series(1, 5);
-- Written by a function a trigger calls.
create table tag_log (id serial primary key, todo_id int not null);
create function log_tag(todo int) returns void language sql as $$ insert into tag_log (todo_id) values (todo) $$;
create function tag_added() returns trigger language plpgsql as $$
begin
  perform log_tag(new.todo_id);
  return new;
end $$;
create trigger tags_added after insert on tags for each row execute function tag_added();
-- Written by SQL a function a trigger calls builds at run time.
create table note_log (id serial primary key, note int not null);
create function log_note(note int) returns void language plpgsql as $$
begin
  execute format('insert into note_log (note) values (%s)', note);
end $$;
create function note_bumped() returns trigger language plpgsql as $$
begin
  perform log_note(new.id);
  return new;
end $$;
create trigger notes_bumped after update on tenant.notes for each row execute function note_bumped();
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
