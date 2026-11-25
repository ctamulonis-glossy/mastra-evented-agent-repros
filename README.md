# Mastra evented and durable agent reproductions

Bugs and missing features in `createEventedAgent` and `createDurableAgent` with several replicas on
shared Postgres and Redis Streams. Each script starts its own replicas as child processes and exits
1 while the problem exists. The model is scripted (`MockLanguageModelV3`), so no API key is needed.

```bash
docker compose -p mastra-evented-repros up -d   # Postgres :55432, Redis :56379
npm install
```

All reproduce on `@mastra/core` 1.85.0 (pinned here) and on 1.86.0-alpha.7 with the matching
alpha packages.

## 1. `signal-lost.ts`: signals to a running run are lost when the replica that started it dies

```bash
node signal-lost.ts                                            # SIGKILL the starting replica
REPRO_KILL=TERM REPRO_SHUTDOWN=workers-first node signal-lost.ts   # stopWorkers(), then shutdown()
REPRO_KILL=NONE node signal-lost.ts                            # control: passes
REPRO_KILL=TERM node signal-lost.ts                            # control: plain shutdown() passes
```

Replica `a` starts a run with three 4 s tool calls. While the first tool runs, replica `main` sends
`sendSignal({ type: 'user' }, { ifActive: { behavior: 'deliver' }, ifIdle: { behavior: 'wake' } })`
(accepted as `deliver`) and an urgent `sendNotificationSignal()` (record `delivered`). Then `a` is
killed. `main`'s workers finish the run, but no later model call sees either message.

## 2. `channel-render.ts`: a Slack-channel run fails on the first step another replica runs

```bash
node channel-render.ts                     # evented: Slack gets "❌ Error: render.wrapStream is not a function"
REPRO_KIND=durable node channel-render.ts  # control (one replica runs every step): passes
REPRO_FIX=1 node channel-render.ts         # workaround: passes
```

The script is a fake Slack Web API (`apiUrl`). It starts replicas `a` and `b`, each serving the
channel webhook through `getChannels().handleWebhookEvent('slack', request)`, and sends a signed
`app_mention` to `a`. No replica dies.

## 3. `persist-misroute.ts`: `ifIdle: 'persist'` from another replica skips a running run

```bash
node persist-misroute.ts                     # evented: BUG, exits 1
REPRO_KIND=durable node persist-misroute.ts  # durable: BUG, exits 1
```

Replica `a` starts a run with three 4 s tool calls. During the first one, both `a` and `main` send
`sendSignal({ type: 'user' }, { ifActive: { behavior: 'deliver' }, ifIdle: { behavior: 'persist' } })`.
`a`'s signal is accepted as `deliver` and reaches the next model call. `main`'s is accepted as
`persist`: it is stored in the thread for a later turn, and the running run never sees it. With
`ifIdle: { behavior: 'wake' }` the same signal from `main` is delivered (`signal-lost.ts` with
`REPRO_KILL=NONE`).

## 4. `durable-handover.ts`: durable runs on a rolling restart (feature requests)

```bash
node durable-handover.ts                          # SIGTERM + shutdown(): the half-done tool call runs again
REPRO_DRAIN_MS=3000 node durable-handover.ts      # same with shutdown({ drainTimeout: 3000 })
REPRO_MODE=kill node durable-handover.ts          # SIGKILL, no recovery timer: never recovered
REPRO_MODE=kill REPRO_RECOVER=1 node durable-handover.ts   # control: recovered (the tool reruns, as expected after a crash)
```

Mastra has `recovery: { durableAgents: 'auto' }`. Replica `a` starts a run whose first tool call
takes 8 s and gets SIGTERM (or SIGKILL) 1 s into it. Each run uses a fresh database, because
recovery re-drives every orphaned run it finds.
