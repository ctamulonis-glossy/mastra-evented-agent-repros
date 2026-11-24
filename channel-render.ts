// A Slack-channel run on an evented agent fails as soon as a step runs on another replica:
// `render.wrapStream is not a function`, and Slack gets "❌ Error: ..." instead of the reply.
// No replica dies. This process is a fake Slack Web API; it starts two replicas ("a", "b"), each
// serving the channel webhook, and sends a signed app_mention to "a". Exits 1 while the bug exists.
// REPRO_KIND=durable runs everything on "a" (control). REPRO_FIX=1 adds the workaround.
import { spawn, type ChildProcess } from 'node:child_process';
import { createHmac } from 'node:crypto';
import { createServer } from 'node:http';
import { RequestContext } from '@mastra/core/request-context';
import { createTool } from '@mastra/core/tools';
import { createSlackAdapter } from '@mastra/slack';
import { z } from 'zod';
import { PREFIX, buildMastra, log, scriptedModel, sleep } from './common.ts';

const SECRET = 'repro-signing-secret';
const SLACK_PORT = 57010;
const port = (replica: string) => (replica === 'a' ? 57011 : 57012);

if (process.argv[2] === 'replica') {
  const work = createTool({ id: 'work', description: 'Does one part of the work.', inputSchema: z.object({ part: z.number() }), execute: async ({ part }) => { await sleep(1500); return { part }; } });
  const { inner } = await buildMastra({
    instructions: 'Do the work in parts.',
    model: scriptedModel((k) => (k < 3 ? { toolCall: { name: 'work', input: { part: k } } } : { text: 'DONE' })),
    tools: { work },
    // Workaround: drop a render context restored from the run's JSON snapshot (functions stripped),
    // so the channel rebuilds it from the thread.
    outputProcessors: process.env.REPRO_FIX === '1' ? [{
      id: 'drop-stale-render',
      processOutputStream: async ({ part, requestContext }: any) => {
        const render = requestContext?.get('__mastra_chat_channel_render');
        if (render && typeof render.wrapStream !== 'function') requestContext.delete('__mastra_chat_channel_render');
        return part;
      },
    }] : [],
    channels: {
      adapters: { slack: createSlackAdapter({ mode: 'webhook', signingSecret: SECRET, botToken: 'xoxb-fake', botUserId: 'U_BOT', apiUrl: `http://localhost:${SLACK_PORT}/api/` }) },
      threadContext: { maxMessages: 0 },
    },
  } as never);
  void RequestContext;
  createServer(async (req, res) => {
    const body = await new Promise<string>((r) => { let b = ''; req.on('data', (c) => (b += c)); req.on('end', () => r(b)); });
    const headers = new Headers(Object.entries(req.headers).map(([k, v]) => [k, String(v)]) as [string, string][]);
    const response = await inner.getChannels().handleWebhookEvent('slack', new Request(`http://localhost${req.url}`, { method: 'POST', headers, body }));
    res.writeHead(response.status).end(await response.text());
  }).listen(port(process.env.REPLICA!), () => log('ready'));
} else {
  // Fake Slack Web API: every method answers ok; posts are recorded.
  const posts: Array<{ replica: string; method: string; text: string }> = [];
  let ts = 0;
  createServer(async (req, res) => {
    const raw = await new Promise<string>((r) => { let b = ''; req.on('data', (c) => (b += c)); req.on('end', () => r(b)); });
    const method = req.url!.replace(/^\/api\//, '').split('?')[0]!;
    const args: Record<string, any> = req.headers['content-type']?.includes('json') ? JSON.parse(raw || '{}') : Object.fromEntries(new URLSearchParams(raw));
    const body: Record<string, unknown> = { ok: true };
    if (method === 'auth.test') Object.assign(body, { user_id: 'U_BOT', bot_id: 'B_BOT', team_id: 'T1' });
    if (method === 'users.info') Object.assign(body, { user: { id: args.user, name: 'alice', profile: { display_name: 'Alice' } } });
    if (method === 'conversations.info') Object.assign(body, { channel: { id: args.channel, name: 'general', is_channel: true } });
    if (/conversations\.(replies|history)/.test(method)) Object.assign(body, { messages: [] });
    if (/chat\.(postMessage|update)/.test(method)) {
      const t = args.ts ?? `1800000000.${String(++ts).padStart(6, '0')}`;
      posts.push({ replica: String(req.headers['user-agent'] ?? ''), method, text: String(args.text || args.markdown_text || JSON.stringify(args.blocks ?? '')) });
      Object.assign(body, { channel: args.channel, ts: t, message: { ts: t, text: args.text } });
    }
    res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(body));
  }).listen(SLACK_PORT);

  const replicas: ChildProcess[] = ['a', 'b'].map((r) => spawn(process.execPath, [process.argv[1]!, 'replica'], { env: { ...process.env, REPLICA: r, REPRO_PREFIX: PREFIX }, stdio: 'inherit' }));
  await sleep(4000);
  const eventTs = `1700000000.${String(Date.now()).slice(-6)}`;
  const event = JSON.stringify({ token: 'x', team_id: 'T1', api_app_id: 'A1', type: 'event_callback', event_id: `Ev${Date.now()}`, event_time: Math.floor(Date.now() / 1000),
    event: { type: 'app_mention', user: 'U_ALICE', text: '<@U_BOT> do the work', ts: eventTs, event_ts: eventTs, channel: 'C_GENERAL', team: 'T1' } });
  const timestamp = String(Math.floor(Date.now() / 1000));
  const signature = 'v0=' + createHmac('sha256', SECRET).update(`v0:${timestamp}:${event}`).digest('hex');
  const ack = await fetch(`http://localhost:${port('a')}/`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-slack-request-timestamp': timestamp, 'x-slack-signature': signature }, body: event });
  log('webhook ack', ack.status);
  for (let i = 0; i < 60 && !posts.some((p) => /DONE|Error/.test(p.text)); i++) await sleep(500);
  await sleep(2000);
  for (const p of posts) log(`slack ${p.method}: ${JSON.stringify(p.text.slice(0, 80))}`);
  const done = posts.some((p) => p.text.includes('DONE'));
  const error = posts.find((p) => p.text.includes('Error'));
  console.log(done && !error ? 'OK: the reply reached Slack' : `BUG: ${error ? `Slack got ${JSON.stringify(error.text)}` : 'no reply reached Slack'}`);
  for (const r of replicas) r.kill('SIGKILL');
  process.exit(done && !error ? 0 : 1);
}
