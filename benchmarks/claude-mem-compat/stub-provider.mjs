// A stand-in for claude-mem's generation provider: no model, no API key, no spend.
//
// claude-mem loads it in the generation worker through its own extension point,
// CLAUDE_MEM_SERVER_PROVIDER=custom with CLAUDE_MEM_CUSTOM_PROVIDER_MODULE=<this file>
// (src/server/runtime/create-server-service.ts, loadCustomServerGenerationProvider).
// Everything else in the worker (the BullMQ Worker, retries, backoff, the Postgres
// outbox) is claude-mem's own code.
//
// What it answers is decided by the `stub` field of the event's payload, so the harness
// can drive each queue path on purpose:
//
//   ok                 one observation, at once
//   slow               one observation after 3 s (used to keep jobs in flight)
//   flaky              a rate-limit error on the first attempt, an observation after.
//                      A rate limit is the error claude-mem treats as retryable, so this
//                      takes BullMQ's retry path: the job goes to `delayed` and comes back.
//   always-rate-limit  a rate-limit error on every attempt, so the job runs out of attempts
//   reject-once        a non-retryable error until an operator retries the job
//                      (POST /v1/jobs/:id/retry), an observation after
//
// A session summary job gets one observation. Each call is logged to stdout with
// "[stub-provider]", which is how the harness counts calls.

function markerOf(context) {
  for (const event of context.events ?? []) {
    const payload = event?.payload;
    if (payload && typeof payload === 'object' && typeof payload.stub === 'string') {
      return payload.stub;
    }
  }
  return 'ok';
}

function observation(title, fact) {
  return [
    '<observation>',
    '  <type>discovery</type>',
    `  <title>${title}</title>`,
    `  <facts><fact>${fact}</fact></facts>`,
    '</observation>',
  ].join('\n');
}

export function createProvider(helpers) {
  const { ServerClassifiedProviderError } = helpers;
  const rateLimit = (message) =>
    new ServerClassifiedProviderError(message, { kind: 'rate_limit', cause: null });

  return {
    providerLabel: 'claude',
    async generate(context) {
      const job = context.job;
      const marker = job.sourceType === 'session_summary' ? 'summary' : markerOf(context);
      const retried = Number(job.payload?.retried_count ?? 0);
      console.log(`[stub-provider] job=${job.id} source=${job.sourceType} marker=${marker} attempt=${job.attempts} retried=${retried}`);

      if (marker === 'flaky' && job.attempts < 2) {
        throw rateLimit('stub: rate limited on the first attempt');
      }
      if (marker === 'always-rate-limit') {
        throw rateLimit('stub: rate limited on every attempt');
      }
      if (marker === 'reject-once' && retried === 0) {
        throw new Error('stub: rejected until an operator retries');
      }
      if (marker === 'slow') {
        await new Promise((resolve) => setTimeout(resolve, 3000));
      }
      const subject = job.sourceType === 'session_summary' ? `session ${job.sourceId}` : `event ${job.sourceId}`;
      return {
        rawText: observation(`stub ${marker} ${job.sourceId}`, `stub observation for ${subject} (${marker})`),
        providerLabel: 'claude',
        modelId: 'stub-provider',
        tokensUsed: 0,
      };
    },
  };
}

export default { createProvider };
