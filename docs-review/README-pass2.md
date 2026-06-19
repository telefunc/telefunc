# Docs sentence-review — Pass 2 (raise kept 7–8 sentences toward 10/10)

Follow-up to the pass-1 review of the docs introduced by PR #264. Pass 1 rewrote every sentence
rated Overall ≤ 7. **Pass 2** takes the sentences that pass 1 *kept* at **7 or 8** — clear and
natural, but each with a real (if minor) documented weakness — and pushes them toward 10/10.

- Scope was confirmed with the maintainer: **only the 7–8 sentences** (the 348 sentences rated 9
  were left untouched — a 9 is a high rating, and reworking near-perfect prose risks churn).
- Method: [`METHODOLOGY-pass2.md`](./METHODOLOGY-pass2.md).
- Review artifact only — safe to drop before merge.
- `node docs/check-docs.mjs` passes (52 pages, 82 anchor links resolve); changes are prose-only.

## Results at a glance

| Batch | Files | Targets | Applied | Retained |
|---|---|---:|---:|---:|
| [A](./pass2-A-stream.md) | stream | 12 | 9 | 3 |
| [B](./pass2-B-channel.md) | channel | 5 | 4 | 1 |
| [C](./pass2-C-cf-scale-transport.md) | stream/cloudflare, stream/scale, transport | 15 | 14 | 1 |
| [D](./pass2-D-files.md) | file-download, file-upload | 7 | 6 | 1 |
| [E](./pass2-E-rxjs-tanstack.md) | rxjs, tanstack-query | 15 | 15 | 0 |
| [F](./pass2-F-api-core.md) | server, getContext, close, serve, Telefunc | 18 | 16 | 2 |
| [G](./pass2-G-misc.md) | redis, redis/README, testing, NeedsLongRunningServer, CONTRIBUTING | 6 | 6 | 0 |
| **Total** | **18 files** | **78** | **70** | **8** |

Each applied edit raised the sentence from 7–8 to 9 or 10. Each *retained* sentence was tried
(candidates documented) but no rewrite genuinely beat the original without losing meaning or voice.

## The 8 retained sentences (tried, kept as-is)

- **stream** — `[12]` "most performant available transport (HTTP, SSE, WebSocket)"; `[36]` the
  `BroadcastChannel` "bridged onto a `Broadcast` key" bullet; `[57]` "cross-instance broadcast
  transport" — each uses precise, load-bearing terminology that any reword would weaken.
- **channel** — `[89]` the custom-transport "subscriber multiplexing and same-node delivery"
  sentence (jargon is precise and the clause already explains it).
- **cloudflare/scale/transport** — `[72]` "fail unpredictably" (sharpening it would add a
  failure-mode fact not in the source).
- **file-download/upload** — `[44]` the "Publish-side only" table cell (terse by table convention).
- **server/getContext/close/serve/Telefunc** — close `[5]` "Everything in the returned value
  (walked recursively)"; Telefunc `[32]` Methods-table cell (its long parenthetical carries
  load-bearing input-shape examples).

## Integrator adjustment

One agent edit was not a genuine improvement and was revised by hand:

- **stream** rxjs link gloss — the agent changed "reactive operators" → "streams as reactive
  operators" (awkward; operators aren't streams). Set to **"reactive streams and operators"**
  instead, which names both what the integration gives you (Observables/Subjects) and the operators.

## Note on the Cloudflare hibernation sentence

The hibernation-preconditions sentence (shipped via the earlier companion PR #370) got one further
parallelism tweak here: "once all channels **close**, no clients remain connected" →
"once all channels **are closed**, no clients remain connected" (passive matches the adjacent
"remain connected"). The apparent redundancy flagged in pass 1 is left for the maintainer — it's a
content question, not a wording one.
