export { ROLES, SCHEMA, seed, USERS }

import { CODES, REFS } from './spellings.ts'

const USERS = 6

/** The same tables for every ORM; each run starts from them. */
const SCHEMA = `
drop table if exists tags, todos, users, deleted_todos, notes, tag_log, note_log, events, event_log, counters, settled,
  resets, measures, chained, brands, products, metrics cascade;
drop sequence if exists tickets;
drop schema if exists tenant cascade;
drop function if exists log_deleted_todo, log_tag, tag_added, log_note, note_bumped, next_event, settle_event,
  event_inserted, empty_event_log, stamp, reset_stamped, label_of, settle_stably, measure_set;
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
-- A write to \`events\` runs code that writes other tables: a rule, a column default calling a function, and a
-- deferred constraint trigger calling one. A trigger empties \`event_log\` on each write to \`resets\`.
create table counters (id int primary key, n int not null);
insert into counters values (1, 0), (2, 0);
create table settled (id int primary key, n int not null);
insert into settled values (1, 0);
create table event_log (id serial primary key, kind text not null);
create function next_event() returns int language sql as $$ update counters set n = n + 1 where id = 1 returning n $$;
create table events (id serial primary key, kind text not null, seq int not null default next_event());
create rule events_logged as on insert to events do also insert into event_log (kind) values (new.kind);
create function settle_event() returns void language sql as $$ update settled set n = n + 1 $$;
create function event_inserted() returns trigger language plpgsql as $$
begin
  perform settle_event();
  return new;
end $$;
create constraint trigger events_settled after insert on events deferrable initially deferred
  for each row execute function event_inserted();
create table resets (id serial primary key);
create function empty_event_log() returns trigger language plpgsql as $$
begin
  truncate event_log;
  return null;
end $$;
create trigger resets_emptied after insert on resets for each statement execute function empty_event_log();
-- A STABLE function whose default argument writes \`counters\`: the caller evaluates it. A trigger on \`resets\` calls
-- it, and so does a read.
create function stamp(n int default next_event()) returns int language sql stable as $$ select n $$;
create function reset_stamped() returns trigger language plpgsql as $$
begin
  perform stamp();
  return null;
end $$;
create trigger resets_stamped after insert on resets for each statement execute function reset_stamped();
-- Written only after a \`COMMIT AND CHAIN\`.
create table chained (id int primary key, n int not null);
insert into chained values (1, 0);
-- Each read of the next ticket takes one: identical reads of it never share a run.
create sequence tickets;
-- One row per parameter of \`METRICS\`: a read with one of them can't share a run with a read with another.
create table metrics (id int primary key, metric float8);
insert into metrics values (1, 'NaN'), (2, null), (3, 'Infinity'), (4, '-Infinity');
-- A product references its brand by a slug the brand's name generates: renaming the brand moves its products.
create table brands (id int primary key, name text not null, slug text generated always as (lower(name)) stored unique);
create table products (id int primary key, brand text references brands (slug) on update cascade);
insert into brands (id, name) values (1, 'Acme'), (2, 'Globex'), (3, 'Initech');
insert into products values (1, 'acme'), (2, 'acme'), (3, 'globex'), (4, 'initech');
-- A STABLE helper a live read calls: the read stays a read.
create function label_of(t text) returns text language sql stable as $$ select upper(t) $$;
-- Whole numbers past a float8's precision: compared with a float8, they compare as float8s, rounded.
create table measures (id int primary key, value numeric not null);
insert into measures values (1, 9007199254740992), (2, 9007199254740994);
-- A STABLE function whose body calls a writing one: a trigger on \`measures\` calls it, and writes \`settled\`.
create function settle_stably() returns void language sql stable as $$ select settle_event() $$;
create function measure_set() returns trigger language plpgsql as $$
begin
  perform settle_stably();
  return null;
end $$;
create trigger measures_set after update on measures for each statement execute function measure_set();
`

/** A role that may write `todos` but not read it all, for `ROLES=1` runs: a write's widened `RETURNING` would be
 *  refused. Roles are the server's, shared by its databases: created once, never dropped. After one of these runs a
 *  statement, no write is widened: `ROLES` runs measure the rest less precisely. */
const ROLES = `
do $$ begin create role live_restricted; exception when duplicate_object then null; end $$;
grant insert (user_id, text), select (id) on todos to live_restricted;
grant usage on sequence todos_id_seq to live_restricted;
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
