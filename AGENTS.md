CRITICALLY IMPORTANT — MANDATORY FOR EVERY RESPONSE:
Before answering ANY question, you MUST enumerate every relevant fact, constraint, and contextual detail from the user's message. Write them out explicitly. Then — and only then — derive your answer from ALL of those constraints together. If your answer ignores any constraint you listed, your answer is wrong. Never pattern-match on a single surface feature. Never give a reflexive answer. If the answer feels instant and obvious, that is a signal to slow down — you are probably ignoring context.

## Workflow
- After making code changes, run `pnpm run -w format`.
- Never run mutating git commands (`stash`, `reset`, `checkout`, `commit`, `push`) unless explicitly asked. Use `git show HEAD:path` / `git diff` to inspect.
- Don't seek permission for obvious follow-through cleanup — just do it.

## Acting on user requests
- **Treat a flagged line as a class of problem.** When the user points at one bad instance, grep the whole repo (incl. `docs/`, `test/`, `playground/`, examples) and fix every instance in the same pass.
- **Never defer or scope-limit.** No "that's a separate change" / "want me to do this later?" — do it now. Never dismiss a question as "out of scope" or claim you're "just a code assistant." If you lack context, ask for it. If you need clarification, ask. Engage with every question the user asks.
- **Never be lazy with cleanup.** Dead code, stale fallbacks, inconsistencies between client/server: fix immediately when spotted, in the same session.
- **After any type/signature change**, grep all references and remove anything just passed-through-but-never-read. Replace silent fallbacks for impossible cases with `assert`.
- **Don't reflexively agree.** When the user pushes back, think it through independently first. If they're right, explain why. If they're wrong, explain why. Don't mirror.

## Code style — non-negotiable
- **Cleanest possible architecture, always.** No "good enough", no workarounds. If the design needs a hack, change the design.
- **Each type/value file owns its own logic end-to-end** (detection, serialization, pump, cleanup). Registries are thin loops — iterate and delegate, no branching.
- **No indirection helpers** that just delegate. Inline the logic.
- **Symmetric naming and structure** between client/server, inline/channel, request/response. Methods on paired classes (e.g. `ClientChannel`/`ServerChannel`) should be in matching order.
- **Explicit type contracts.** No `unknown` casts. No `Record<string, unknown>` as a workaround. No `as` casts unless at a true boundary (`JSON.parse`, external APIs) — verify the generic doesn't already preserve the type.
- **No type aliases that just rename another type** (`type X = Y`). Import the original everywhere. If the original name doesn't fit, fix the original name.
- **Top-down file order.** Public/exported function near the top; private helpers below in call order. Caller above callee.
- **Never regress performance.** Preserve O(1) amortized ops, single-pass eviction, parallel-array layouts. Keep existing comments. When splitting a structure, duplicate the pattern faithfully — don't rewrite the algorithm.

## Completion integrity — non-negotiable
- **Never claim work is done without running verification.** After code changes: run the build, run the type-checker, run the relevant tests. Include the command output. If you can't verify (no test exists, CI is broken), say exactly what you couldn't verify and why.
- **Never fabricate results.** If you didn't run a command, don't describe its output. If you didn't read a file, don't quote it. If a test failed, show the failure — don't say it passed.
- **Never stop mid-feature and call it complete.** If a task has multiple steps, finish all of them. If you genuinely can't continue (blocked, need input, hitting a wall), list every unfinished step explicitly. "The rest is straightforward" is not acceptable — if it's straightforward, do it.
- **If you're unsure whether something works, that means it doesn't work yet.** Uncertainty = unverified = not done.
