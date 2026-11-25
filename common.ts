// Shared setup: an agent with a scripted model (no API key), wrapped by createEventedAgent, on a
// Mastra with Postgres storage and Redis Streams pubsub + Redis replay cache. Every process that
// calls buildMastra() is one "replica"; they share Postgres and Redis.
import { Agent } from '@mastra/core/agent';
import { createDurableAgent, createEventedAgent } from '@mastra/core/agent/durable';
import { Mastra } from '@mastra/core/mastra';
import { Memory } from '@mastra/memory';
import { PostgresStore } from '@mastra/pg';
import { RedisServerCache, nodeRedisPreset } from '@mastra/redis';
import { RedisStreamsPubSub } from '@mastra/redis-streams';
import { MockLanguageModelV3, simulateReadableStream } from 'ai/test';
import pg from 'pg';
import { createClient } from 'redis';

export const PG_URL = process.env.PG_URL ?? 'postgres://postgres:repro@localhost:55432/repro';
export const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:56379';
/** Each script run gets its own Redis key prefix, so earlier runs never interfere. */
export const PREFIX = process.env.REPRO_PREFIX ?? `repro-${Date.now()}`;
/** `durable` swaps createEventedAgent for createDurableAgent, as a control. */
export const KIND = process.env.REPRO_KIND ?? 'evented';
export const REPLICA = process.env.REPLICA ?? 'main';

export const db = new pg.Pool({ connectionString: PG_URL, max: 4 });
export const log = (...args: unknown[]) => console.log(`${new Date().toISOString().slice(11, 23)} [${REPLICA}]`, ...args);

const usage = { inputTokens: { total: 1, noCache: 1, cacheRead: 0, cacheWrite: 0 }, outputTokens: { total: 1, text: 1, reasoning: 0 } };
type Step = { toolCall?: { name: string; input: unknown }; text?: string };

/**
 * A model that plays `script(prompt)`: the prompt's tool results decide the step. Each call is
 * logged with the MARK-... strings it can see, so a script can tell what reached the model.
 */
export function scriptedModel(script: (toolResults: number) => Step) {
  return new MockLanguageModelV3({
    doStream: async ({ prompt }) => {
      const k = prompt.filter((m) => m.role === 'tool').length;
      const saw = [...new Set(JSON.stringify(prompt).match(/MARK-[A-Za-z0-9]+/g) ?? [])];
      log(`model call #${k}`, saw.length ? `sees ${saw.join(', ')}` : '');
      await db.query('insert into repro_model_calls (prefix, replica, k, saw) values ($1, $2, $3, $4)', [PREFIX, REPLICA, k, saw]);
      const step = script(k);
      const chunks: unknown[] = [{ type: 'stream-start', warnings: [] }];
      if (step.text) chunks.push({ type: 'text-start', id: `t${k}` }, { type: 'text-delta', id: `t${k}`, delta: step.text }, { type: 'text-end', id: `t${k}` });
      if (step.toolCall) chunks.push({ type: 'tool-call', toolCallId: `call-${k}`, toolName: step.toolCall.name, input: JSON.stringify(step.toolCall.input) });
      chunks.push({ type: 'finish', usage, finishReason: step.toolCall ? { unified: 'tool-calls', raw: 'tool_use' } : { unified: 'stop', raw: 'stop' } });
      return { stream: simulateReadableStream({ chunkDelayInMs: 5, chunks: chunks as never }) };
    },
  });
}

export async function buildMastra(agentConfig: Omit<ConstructorParameters<typeof Agent>[0], 'id' | 'name' | 'memory'> & { channels?: unknown }) {
  await db.query('create table if not exists repro_model_calls (at timestamptz default clock_timestamp(), prefix text, replica text, k int, saw text[])');
  const storage = new PostgresStore({ id: 'repro', connectionString: PG_URL });
  const agent = new Agent({ id: 'repro', name: 'Repro', memory: new Memory({ storage, options: { lastMessages: 50 } }), ...(agentConfig as object) } as never);
  const redis = createClient({ url: REDIS_URL });
  await redis.connect();
  const cache = new RedisServerCache({ client: redis as never }, { ...nodeRedisPreset, keyPrefix: `${PREFIX}:cache` });
  const pubsub = new RedisStreamsPubSub({ url: REDIS_URL, keyPrefix: `${PREFIX}:ps`, reclaimIntervalMs: 5_000, reclaimIdleMs: 30_000 });
  const mastra = new Mastra({
    logger: false,
    agents: { repro: KIND === 'durable' ? createDurableAgent({ agent, cache, pubsub }) : createEventedAgent({ agent, cache }) },
    storage,
    pubsub,
    serverCache: cache,
    // Durable runs write the `running` checkpoints recovery needs only with this on.
    ...(KIND === 'durable' ? { recovery: { durableAgents: 'auto' as const } } : {}),
  });
  await mastra.startWorkers();
  return { mastra, agent: mastra.getAgent('repro') as any, inner: agent as any };
}

export const modelCalls = async () => (await db.query('select replica, k, saw from repro_model_calls where prefix = $1 order by at', [PREFIX])).rows as Array<{ replica: string; k: number; saw: string[] }>;
export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
