// Two durable-agent (createDurableAgent) behaviours on a rolling restart, for feature requests.
// Replica "a" (a child) starts a run whose first tool call takes 8 s; this process is replica "main".
//
// REPRO_MODE=term (default): "a" gets SIGTERM 1 s into the tool and runs mastra.shutdown() (default
//   drainTimeout). "main" calls recoverAllDurableAgents() every 2 s. The run finishes on "main", but
//   the half-done tool call runs again from the start: there is no way to stop "a" at the next step
//   boundary and hand the run over there.
// REPRO_MODE=kill: "a" gets SIGKILL. "main" has started its workers but never calls
//   recoverAllDurableAgents() itself (only Mastra's own server does, once, on boot), so the run is
//   never picked up. REPRO_RECOVER=1 adds the 2 s recovery timer as the control.
// Exits 1 when the tool ran twice or the run never finished.
import { spawn } from 'node:child_process';
import { RequestContext } from '@mastra/core/request-context';
import { createTool } from '@mastra/core/tools';
import { z } from 'zod';

process.env.REPRO_KIND = 'durable'; // read by common.ts at import
if (process.argv[2] !== 'child') {
  // A fresh database per run: recovery re-drives every orphaned run it finds, earlier runs' included.
  const base = process.env.PG_URL ?? 'postgres://postgres:repro@localhost:55432/repro';
  const name = `repro_dh_${Date.now()}`;
  const admin = new (await import('pg')).default.Client({ connectionString: base });
  await admin.connect(); await admin.query(`create database ${name}`); await admin.end();
  process.env.PG_URL = base.replace(/\/[^/]*$/, `/${name}`);
}
const { PREFIX, REPLICA, buildMastra, db, log, modelCalls, scriptedModel, sleep } = await import('./common.ts');
const MODE = process.env.REPRO_MODE ?? 'term';
const STEPS = 2;
await db.query('create table if not exists repro_tool_runs (at timestamptz default clock_timestamp(), prefix text, replica text, part int, phase text)');
const work = createTool({
  id: 'work',
  description: 'Does one part of the work.',
  inputSchema: z.object({ part: z.number() }),
  execute: async ({ part }) => {
    await db.query('insert into repro_tool_runs (prefix, replica, part, phase) values ($1, $2, $3, $4)', [PREFIX, REPLICA, part, 'start']);
    await sleep(part === 0 ? 8000 : 500);
    await db.query('insert into repro_tool_runs (prefix, replica, part, phase) values ($1, $2, $3, $4)', [PREFIX, REPLICA, part, 'end']);
    return { part };
  },
});
const { mastra, agent } = await buildMastra({
  instructions: 'Do the work in parts.',
  model: scriptedModel((k) => (k < STEPS ? { toolCall: { name: 'work', input: { part: k } } } : { text: 'DONE' })),
  tools: { work },
});
const thread = process.env.REPRO_THREAD ?? `thread-${Date.now()}`;

if (process.argv[2] === 'child') {
  process.on('SIGTERM', async () => { log('SIGTERM: mastra.shutdown()'); await mastra.shutdown(process.env.REPRO_DRAIN_MS ? { drainTimeout: Number(process.env.REPRO_DRAIN_MS) } : undefined); log('shutdown done'); process.exit(0); });
  const run = await agent.stream('Do the work.', { memory: { thread, resource: 'user' }, requestContext: new RequestContext() });
  log('started run', run.runId);
  for await (const _ of run.output.fullStream);
} else {
  if (MODE === 'term' || process.env.REPRO_RECOVER === '1') {
    setInterval(async () => { const r = await mastra.recoverAllDurableAgents(); if (r.recovered || r.skipped || r.failed) log('recoverAllDurableAgents:', JSON.stringify(r)); }, 2000);
  }
  const child = spawn(process.execPath, [process.argv[1]!, 'child'], { env: { ...process.env, REPLICA: 'a', REPRO_PREFIX: PREFIX, REPRO_THREAD: thread }, stdio: 'inherit' });
  const toolRuns = async () => (await db.query('select replica, part, phase from repro_tool_runs where prefix = $1 order by at', [PREFIX])).rows as Array<{ replica: string; part: number; phase: string }>;
  while (!(await toolRuns()).length) await sleep(100);
  await sleep(1000);
  const signal = MODE === 'kill' ? 'SIGKILL' : 'SIGTERM';
  log(`${signal} to replica a, 1 s into the 8 s tool call`);
  child.kill(signal);
  let final = false;
  for (let i = 0; i < 120 && !final; i++) { await sleep(500); final = (await modelCalls()).some((c) => c.k === STEPS); }
  const runs = await toolRuns();
  log('tool runs:', runs.map((r) => `${r.phase}(${r.part})@${r.replica}`).join(' '));
  const part0Starts = runs.filter((r) => r.part === 0 && r.phase === 'start').length;
  console.log(!final ? 'NOT RECOVERED: the run never finished within 60 s' : part0Starts > 1 ? `RERUN: the first tool call started ${part0Starts} times` : 'OK: finished, every tool call ran once');
  child.kill('SIGKILL');
  await mastra.shutdown().catch(() => {});
  process.exit(final && part0Starts === 1 ? 0 : 1);
}
