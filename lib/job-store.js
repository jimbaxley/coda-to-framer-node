const jobs = new Map();
const jobEvents = new Map();

const ACTIVE_STATUSES = new Set(["queued", "delayed", "running", "publishing"]);

function nowIso() {
  return new Date().toISOString();
}

export function createJob({ jobId, requestId, idempotencyKey, payload }) {
  const createdAt = nowIso();
  const job = {
    jobId,
    requestId,
    idempotencyKey: idempotencyKey || "",
    status: "queued",
    createdAt,
    updatedAt: createdAt,
    startedAt: "",
    completedAt: "",
    payload,
    result: null,
    error: null,
  };

  jobs.set(jobId, job);
  jobEvents.set(jobId, [
    {
      at: createdAt,
      level: "info",
      stage: "queued",
      message: "Job accepted",
      details: {
        action: payload.action || "sync",
      },
    },
  ]);

  return job;
}

export function getJob(jobId) {
  return jobs.get(jobId) || null;
}

export function updateJob(jobId, patch) {
  const current = jobs.get(jobId);
  if (!current) {
    return null;
  }

  const next = {
    ...current,
    ...patch,
    updatedAt: nowIso(),
  };

  jobs.set(jobId, next);
  return next;
}

export function appendJobEvent(jobId, event) {
  const currentEvents = jobEvents.get(jobId) || [];
  const nextEvent = {
    at: nowIso(),
    level: event.level || "info",
    stage: event.stage || "general",
    message: event.message || "",
    details: event.details || {},
  };
  currentEvents.push(nextEvent);
  jobEvents.set(jobId, currentEvents);
  return nextEvent;
}

export function getJobEvents(jobId) {
  return jobEvents.get(jobId) || [];
}

export function findActiveJobByIdempotency(idempotencyKey) {
  if (!idempotencyKey) {
    return null;
  }

  for (const job of jobs.values()) {
    if (job.idempotencyKey === idempotencyKey && ACTIVE_STATUSES.has(job.status)) {
      return job;
    }
  }

  return null;
}

export function getJobWithEvents(jobId) {
  const job = getJob(jobId);
  if (!job) {
    return null;
  }

  return {
    ...job,
    events: getJobEvents(jobId),
  };
}

export function listJobsWithEvents({ limit = 50, cursor = 0 } = {}) {
  const safeLimit = Math.max(1, Math.min(200, Number(limit) || 50));
  const safeCursor = Math.max(0, Number(cursor) || 0);

  const allJobs = Array.from(jobs.values())
    .slice()
    .sort((a, b) => {
      const at = new Date(a.createdAt || 0).getTime();
      const bt = new Date(b.createdAt || 0).getTime();
      return bt - at;
    });

  const page = allJobs.slice(safeCursor, safeCursor + safeLimit);
  const nextCursor = safeCursor + safeLimit < allJobs.length
    ? String(safeCursor + safeLimit)
    : "";

  return {
    jobs: page.map((job) => ({
      ...job,
      events: getJobEvents(job.jobId),
    })),
    continuation: nextCursor ? { cursor: nextCursor } : null,
    total: allJobs.length,
  };
}
