// A signal accepted as `deliver` for a running evented run is lost when the replica that started the
// run dies, although another replica finishes the run. Two processes: this one (replica "main") and
// a child (replica "a") that starts the run. While the run is in its first tool call, main sends a
// user signal and an urgent notification, then kills "a" with SIGKILL. Main's workers finish the
// run; no later model call sees the signal. Exits 1 while the bug exists.
// REPRO_KILL=TERM sends SIGTERM instead (the child then runs mastra.shutdown()).
import { spawn } from 'node:child_process';
import { RequestContext } from '@mastra/core/request-context';
import { createTool } from '@mastra/core/tools';
import { z } from 'zod';
import { PREFIX, buildMastra, log, modelCalls, scriptedModel, sleep } from './common.ts';

const STEPS = 3;
const work = createTool({
  id: 'work',
  description: 'Does one part of the work.',
  inputSchema: z.object({ part: z.number() }),
  execute: async ({ part }) => { await sleep(4000); return { part }; },
});
const { mastra, agent } = await buildMastra({
  instructions: 'Do the work in parts.',
  model: scriptedModel((k) => (k < STEPS ? { toolCall: { name: 'work', input: { part: k } } } : { text: 'DONE' })),
  tools: { work },
});
const thread = process.env.REPRO_THREAD ?? `thread-${Date.now()}`;

if (process.argv[2] === 'child') {
  process.on('SIGTERM', async () => {
    // REPRO_SHUTDOWN=workers-first: stop taking step events and finish the ones in hand, then shut down.
    if (process.env.REPRO_SHUTDOWN === 'workers-first') { log('SIGTERM: mastra.stopWorkers()'); await mastra.stopWorkers({ drainTimeout: 30_000 }); }
    log('SIGTERM: mastra.shutdown()'); await mastra.shutdown(); process.exit(0);
  });
  const run = await agent.stream('Do the work.', { memory: { thread, resource: 'user' }, requestContext: new RequestContext() });
  log('started run', run.runId);
  for await (const _ of run.output.fullStream);
} else {
  const child = spawn(process.execPath, [process.argv[1]!, 'child'], { env: { ...process.env, REPLICA: 'a', REPRO_PREFIX: PREFIX, REPRO_THREAD: thread }, stdio: 'inherit' });
  while (!(await modelCalls()).length) await sleep(100);
  await sleep(500); // the run is now in its first tool call
  const target = { resourceId: 'user', threadId: thread };
  const signal = agent.sendSignal({ type: 'user', id: 'MARK-SIGNAL', contents: 'MARK-SIGNAL' }, { ...target, ifActive: { behavior: 'deliver' }, ifIdle: { behavior: process.env.REPRO_IF_IDLE ?? 'wake' } as never });
  log('sendSignal accepted:', JSON.stringify(await signal.accepted));
  const note = await agent.sendNotificationSignal({ source: 'repro', kind: 'note', priority: 'urgent', summary: 'MARK-NOTIFICATION' }, target);
  log('sendNotificationSignal:', note.decision.action, note.record.status, JSON.stringify(await note.accepted));
  await sleep(1000);
  const kill = process.env.REPRO_KILL === 'TERM' ? 'SIGTERM' : process.env.REPRO_KILL === 'NONE' ? undefined : 'SIGKILL';
  if (kill) { log(`${kill} to replica a`); child.kill(kill); }
  let final = false;
  for (let i = 0; i < 180 && !final; i++) { await sleep(500); final = (await modelCalls()).some((c) => c.k === STEPS); }
  await sleep(1000);
  const calls = await modelCalls();
  log('model calls:', calls.map((c) => `#${c.k}@${c.replica}[${c.saw.join(',')}]`).join(' '));
  const seen = (m: string) => calls.some((c) => c.saw.includes(m));
  const ok = final && seen('MARK-SIGNAL') && seen('MARK-NOTIFICATION');
  console.log(!final ? 'INCONCLUSIVE: the run did not finish' : ok ? 'OK: the run saw both signals' : `BUG: the run finished on another replica without the signal (${seen('MARK-SIGNAL') ? '' : 'MARK-SIGNAL lost '}${seen('MARK-NOTIFICATION') ? '' : 'MARK-NOTIFICATION lost'})`);
  child.kill('SIGKILL');
  await mastra.shutdown().catch(() => {});
  process.exit(ok ? 0 : 1);
}
