import { validateIpcEvent } from "../shared/ipc-protocol.mjs";

export class EventRouter {
  constructor(options) {
    this.logger = options.logger;
    this.aggregator = options.aggregator;
    this.notifier = options.notifier;
  }

  async route(event, context = {}) {
    const validation = validateIpcEvent(event);
    if (!validation.ok) {
      this.logger.warn("drop invalid ipc event", {
        reason: validation.reason,
        agent: context.agentName ?? "unknown"
      });
      return {
        success: false,
        data: null,
        error: validation.reason,
        meta: { dropped: true }
      };
    }

    if (event.type === "heartbeat") {
      this.logger.debug("heartbeat", {
        source: event.source,
        event_id: event.event_id
      });
      return {
        success: true,
        data: { routed: "heartbeat" },
        error: null,
        meta: {}
      };
    }

    if (event.type === "metric") {
      this.logger.debug("metric", {
        source: event.source,
        event_id: event.event_id,
        data: event.data
      });
      return {
        success: true,
        data: { routed: "metric" },
        error: null,
        meta: {}
      };
    }

    if (event.type === "alert_candidate" || event.type === "alert_resolved") {
      const aggregateResult = this.aggregator.ingest(event);
      if (!aggregateResult.notify) {
        this.logger.info("alert suppressed", {
          fingerprint: aggregateResult.fingerprint,
          status: aggregateResult.status,
          source: event.source,
          rule_id: event.rule_id
        });
        return {
          success: true,
          data: aggregateResult,
          error: null,
          meta: { notified: false }
        };
      }

      const notification = {
        source: event.source,
        agent: context.agentName ?? event.source,
        type: event.type,
        event_id: event.event_id,
        level: event.level,
        rule_id: event.rule_id,
        resource_id: event.resource_id,
        title: event.title,
        message: event.message,
        data: event.data,
        fingerprint: aggregateResult.fingerprint,
        alert_state: aggregateResult.status
      };
      const notifyResult = await this.notifier.notify(notification);
      return {
        success: true,
        data: aggregateResult,
        error: null,
        meta: {
          notified: true,
          channels: notifyResult.data?.length ?? 0
        }
      };
    }

    this.logger.warn("unsupported event type", {
      type: event.type,
      source: event.source
    });
    return {
      success: false,
      data: null,
      error: "unsupported event type",
      meta: {}
    };
  }
}
