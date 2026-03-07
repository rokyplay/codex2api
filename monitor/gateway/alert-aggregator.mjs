import { buildFingerprint } from "../shared/ipc-protocol.mjs";

function toNumber(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function cloneEntry(entry) {
  return JSON.parse(JSON.stringify(entry));
}

export class AlertAggregator {
  constructor(config, logger) {
    this.logger = logger;
    this.dedupeTtlMs = toNumber(config?.dedupe_ttl_ms, 900000);
    this.notifyCooldownMs = toNumber(config?.notify_cooldown_ms, 300000);
    this.alerts = new Map();
    this.loadState(config?.state ?? {});
  }

  loadState(stateObject) {
    if (!stateObject || typeof stateObject !== "object") {
      return;
    }
    for (const [fingerprint, value] of Object.entries(stateObject)) {
      if (!value || typeof value !== "object") {
        continue;
      }
      this.alerts.set(fingerprint, value);
    }
  }

  toSerializableState() {
    const data = {};
    for (const [fingerprint, entry] of this.alerts.entries()) {
      data[fingerprint] = cloneEntry(entry);
    }
    return data;
  }

  listActiveAlerts(limit = 20) {
    this.cleanup();
    const rows = [];
    for (const entry of this.alerts.values()) {
      if (!entry || typeof entry !== "object") {
        continue;
      }
      if (entry.state === "resolved") {
        continue;
      }
      rows.push(cloneEntry(entry));
    }
    rows.sort((a, b) => toNumber(b.last_seen_ts, 0) - toNumber(a.last_seen_ts, 0));
    return rows.slice(0, Math.max(0, Number(limit) || 0));
  }

  countActiveAlerts() {
    this.cleanup();
    let count = 0;
    for (const entry of this.alerts.values()) {
      if (!entry || typeof entry !== "object") {
        continue;
      }
      if (entry.state !== "resolved") {
        count += 1;
      }
    }
    return count;
  }

  cleanup(now = Date.now()) {
    for (const [fingerprint, entry] of this.alerts.entries()) {
      if (!entry || typeof entry !== "object") {
        this.alerts.delete(fingerprint);
        continue;
      }
      const lastSeenTs = toNumber(entry.last_seen_ts, 0);
      if (lastSeenTs <= 0) {
        this.alerts.delete(fingerprint);
        continue;
      }
      if (now - lastSeenTs > this.dedupeTtlMs) {
        this.alerts.delete(fingerprint);
      }
    }
  }

  ingest(event) {
    this.cleanup();
    if (event.type === "alert_candidate") {
      return this._ingestCandidate(event);
    }
    if (event.type === "alert_resolved") {
      return this._ingestResolved(event);
    }
    return {
      notify: false,
      status: "ignored",
      fingerprint: null,
      entry: null
    };
  }

  _ingestCandidate(event) {
    const now = Date.now();
    const fingerprint = buildFingerprint(event.source, event.rule_id, event.resource_id);
    const existing = this.alerts.get(fingerprint);
    const isExpired = existing ? now - toNumber(existing.last_seen_ts, 0) > this.dedupeTtlMs : true;

    if (!existing || isExpired) {
      const entry = {
        fingerprint,
        state: "open",
        source: event.source,
        rule_id: event.rule_id,
        resource_id: event.resource_id,
        first_seen_ts: now,
        last_seen_ts: now,
        last_notified_ts: now,
        notify_count: 1,
        suppressed_count: 0,
        last_event_id: event.event_id,
        last_level: event.level,
        last_title: event.title,
        last_message: event.message
      };
      this.alerts.set(fingerprint, entry);
      this.logger.warn("告警已触发", { fingerprint, rule_id: event.rule_id, resource_id: event.resource_id });
      return {
        notify: true,
        status: "open",
        fingerprint,
        entry: cloneEntry(entry)
      };
    }

    existing.last_seen_ts = now;
    existing.last_event_id = event.event_id;
    existing.last_level = event.level;
    existing.last_title = event.title;
    existing.last_message = event.message;

    if (existing.state === "resolved") {
      existing.state = "open";
      existing.last_notified_ts = now;
      existing.notify_count = toNumber(existing.notify_count, 0) + 1;
      return {
        notify: true,
        status: "open",
        fingerprint,
        entry: cloneEntry(existing)
      };
    }

    const inCooldown = now - toNumber(existing.last_notified_ts, 0) < this.notifyCooldownMs;
    if (inCooldown) {
      existing.state = "suppressed";
      existing.suppressed_count = toNumber(existing.suppressed_count, 0) + 1;
      return {
        notify: false,
        status: "suppressed",
        fingerprint,
        entry: cloneEntry(existing)
      };
    }

    existing.state = "open";
    existing.last_notified_ts = now;
    existing.notify_count = toNumber(existing.notify_count, 0) + 1;
    return {
      notify: true,
      status: "open",
      fingerprint,
      entry: cloneEntry(existing)
    };
  }

  _ingestResolved(event) {
    const now = Date.now();
    const fingerprint = buildFingerprint(event.source, event.rule_id, event.resource_id);
    const existing = this.alerts.get(fingerprint);
    if (!existing) {
      return {
        notify: false,
        status: "ignored",
        fingerprint,
        entry: null
      };
    }

    const wasResolved = existing.state === "resolved";
    existing.state = "resolved";
    existing.last_seen_ts = now;
    existing.last_event_id = event.event_id;
    existing.last_level = event.level;
    existing.last_title = event.title;
    existing.last_message = event.message;
    if (!wasResolved) {
      existing.last_notified_ts = now;
      existing.notify_count = toNumber(existing.notify_count, 0) + 1;
    }
    return {
      notify: !wasResolved,
      status: "resolved",
      fingerprint,
      entry: cloneEntry(existing)
    };
  }
}
