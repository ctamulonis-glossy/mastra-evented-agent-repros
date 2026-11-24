# Mastra evented agent reproductions

Two bugs in `createEventedAgent` with several replicas on shared Postgres and Redis Streams. Each
script starts its own replicas as child processes and exits 1 while the bug exists. The model is
scripted (`MockLanguageModelV3`), so no API key is needed.

```bash
docker compose -p mastra-evented-repros up -d   # Postgres :55432, Redis :56379
npm install
```

Both reproduce on `@mastra/core` 1.85.0 (pinned here) and on 1.86.0-alpha.7 with the matching
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
