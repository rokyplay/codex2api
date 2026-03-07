export const IPC_VERSION = "1.0";

export const IPC_EVENT_TYPES = Object.freeze({
  HEARTBEAT: "heartbeat",
  METRIC: "metric",
  ALERT_CANDIDATE: "alert_candidate",
  ALERT_RESOLVED: "alert_resolved"
});

const VALID_LEVELS = new Set(["debug", "info", "warn", "error", "critical"]);

function buildEventId() {
  const rand = Math.random().toString(36).slice(2, 8);
  return `evt_${Date.now()}_${rand}`;
}

export function normalizeLevel(level) {
  if (typeof level !== "string") {
    return "info";
  }
  const normalized = level.toLowerCase();
  return VALID_LEVELS.has(normalized) ? normalized : "info";
}

export function buildFingerprint(source, ruleId, resourceId) {
  return [source, ruleId, resourceId].map((value) => String(value ?? "")).join("|");
}

export function createIpcEvent(input = {}) {
  return {
    version: IPC_VERSION,
    event_id: input.event_id ?? buildEventId(),
    ts: Number(input.ts ?? Date.now()),
    source: String(input.source ?? "unknown"),
    type: String(input.type ?? IPC_EVENT_TYPES.METRIC),
    level: normalizeLevel(input.level ?? "info"),
    rule_id: input.rule_id == null ? "" : String(input.rule_id),
    resource_id: input.resource_id == null ? "global" : String(input.resource_id),
    title: input.title == null ? "" : String(input.title),
    message: input.message == null ? "" : String(input.message),
    data: input.data && typeof input.data === "object" ? input.data : {}
  };
}

export function createAck(eventId, ok = true, errorMessage = "") {
  return {
    type: "ack",
    ts: Date.now(),
    event_id: eventId,
    ok: Boolean(ok),
    error: errorMessage ? String(errorMessage) : ""
  };
}

export function isAckMessage(message) {
  return Boolean(
    message &&
      typeof message === "object" &&
      message.type === "ack" &&
      typeof message.event_id === "string"
  );
}

export function validateIpcEvent(message) {
  if (!message || typeof message !== "object") {
    return { ok: false, reason: "message must be object" };
  }
  if (message.version !== IPC_VERSION) {
    return { ok: false, reason: "invalid version" };
  }
  if (typeof message.event_id !== "string" || !message.event_id) {
    return { ok: false, reason: "missing event_id" };
  }
  if (typeof message.source !== "string" || !message.source) {
    return { ok: false, reason: "missing source" };
  }
  if (typeof message.type !== "string" || !message.type) {
    return { ok: false, reason: "missing type" };
  }
  if (typeof message.ts !== "number" || !Number.isFinite(message.ts)) {
    return { ok: false, reason: "invalid ts" };
  }
  return { ok: true };
}
