// sendSignal({ ifActive: deliver, ifIdle: persist }) from a replica that is not running the run
// answers `persist` although the run is active, so the running run never sees the message.
// Replica "a" (a child) starts a run with three 4 s tool calls and, during the first one, sends a
// signal itself (control). This process (replica "main") sends the same kind of signal at the same
// time. Exits 1 while the bug exists. REPRO_KIND=durable uses createDurableAgent.
import { spawn } from 'node:child_process';
import { RequestContext } from '@mastra/core/request-context';
import { createTool } from '@mastra/core/tools';
import { z } from 'zod';
import { PREFIX, buildMastra, db, log, modelCalls, scriptedModel, sleep } from './common.ts';

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
const target = { resourceId: 'user', threadId: thread };
const send = (id: string) => agent.sendSignal({ type: 'user', id, contents: id }, { ...target, ifActive: { behavior: 'deliver' }, ifIdle: { behavior: 'persist' } });

if (process.argv[2] === 'child') {
  const run = await agent.stream('Do the work.', { memory: { thread, resource: 'user' }, requestContext: new RequestContext() });
  log('started run', run.runId);
  setTimeout(async () => log('sendSignal from the running replica:', JSON.stringify(await send('MARK-LOCAL').accepted)), 1000);
  for await (const _ of run.output.fullStream);
  await sleep(1000);
  process.exit(0);
} else {
  const child = spawn(process.execPath, [process.argv[1]!, 'child'], { env: { ...process.env, REPLICA: 'a', REPRO_PREFIX: PREFIX, REPRO_THREAD: thread }, stdio: 'inherit' });
  while (!(await modelCalls()).length) await sleep(100);
  await sleep(1000); // the run is now in its first tool call
  const accepted = await send('MARK-REMOTE').accepted;
  log('sendSignal from another replica:', JSON.stringify(accepted));
  await new Promise((r) => child.on('exit', r));
  const calls = await modelCalls();
  log('model calls:', calls.map((c) => `#${c.k}@${c.replica}[${c.saw.join(',')}]`).join(' '));
  const seen = (m: string) => calls.some((c) => c.saw.includes(m));
  const ok = seen('MARK-LOCAL') && seen('MARK-REMOTE');
  console.log(ok ? 'OK: the run saw both signals' : `BUG: ${seen('MARK-LOCAL') ? '' : 'MARK-LOCAL lost '}${seen('MARK-REMOTE') ? '' : `MARK-REMOTE lost (accepted as ${JSON.stringify(accepted)})`}`);
  await sleep(3000);
  const stored = (await db.query("select count(*)::int as n from mastra_messages where thread_id = $1 and content::text like '%MARK-REMOTE%'", [thread])).rows[0].n;
  log('MARK-REMOTE messages stored in the thread:', stored);
  await mastra.shutdown().catch(() => {});
  process.exit(ok ? 0 : 1);
}
