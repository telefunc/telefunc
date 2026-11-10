export { SCHEMA, seed, USERS }

const USERS = 6

/** The same tables for every ORM; each run starts from them. */
const SCHEMA = `
drop table if exists tags, todos, users cascade;
create table users (id serial primary key, name text not null, team int not null);
create table todos (
  id serial primary key,
  user_id int not null references users (id) on delete cascade,
  text text not null,
  done boolean not null default false,
  priority int not null default 0,
  amount numeric(12, 2) not null default 0,
  updated_at timestamptz not null default now()
);
create table tags (id serial primary key, todo_id int not null references todos (id) on delete cascade, name text not null);
`

function seed(random: () => number): string {
  const users = Array.from({ length: USERS }, (_, i) => `('user ${i + 1}', ${(i % 3) + 1})`)
  const todos = Array.from({ length: 40 }, (_, i) => {
    const user = 1 + Math.floor(random() * USERS)
    const text = `${'abc'[i % 3]}${i}`
    const at = `'2026-11-10 08:00:00+00'::timestamptz + interval '${Math.floor(random() * 1000)} seconds'`
    return `(${user}, '${text}', ${random() < 0.4}, ${Math.floor(random() * 5)}, ${(random() * 100).toFixed(2)}, ${at})`
  })
  const tags = Array.from({ length: 20 }, (_, i) => `(${1 + Math.floor(random() * 40)}, 'tag ${i % 4}')`)
  return `${SCHEMA}
insert into users (name, team) values ${users.join(', ')};
insert into todos (user_id, text, done, priority, amount, updated_at) values ${todos.join(', ')};
insert into tags (todo_id, name) values ${tags.join(', ')};`
}
