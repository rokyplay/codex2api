import { fork } from "node:child_process";
import { createIpcEvent, createAck, isAckMessage, validateIpcEvent } from "../shared/ipc-protocol.mjs";

function nowMs() {
  return Date.now();
}

function buildStatusRequestId() {
  return `status_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function sanitizeNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export class ChildSupervisor {
  constructor(options) {
    this.logger = options.logger;
    this.projectRoot = options.projectRoot;
    this.agents = Array.isArray(options.agents) ? options.agents : [];
    this.onEvent = typeof options.onEvent === "function" ? options.onEvent : async () => {};
    this.restartWindowMs = sanitizeNumber(options.restart_window_ms, 10 * 60 * 1000);
    this.restartCriticalThreshold = sanitizeNumber(options.restart_critical_threshold, 5);
    this.heartbeatTimeoutMs = sanitizeNumber(options.heartbeat_timeout_ms, 10 * 60 * 1000);
    this.isShuttingDown = false;
    this.children = new Map();
    this.pendingStatusRequests = new Map();
    this._loadInitialState(options.initialState ?? {});
  }

  _loadInitialState(initialState) {
    for (const agent of this.agents) {
      const cached = initialState?.[agent.name] ?? {};
      this.children.set(agent.name, {
        spec: agent,
        process: null,
        pid: null,
        restartCount: sanitizeNumber(cached.restart_count, 0),
        restartHistory: Array.isArray(cached.restart_history) ? cached.restart_history : [],
        nextBackoffMs: sanitizeNumber(cached.next_backoff_ms, 2000),
        lastHeartbeatTs: sanitizeNumber(cached.last_heartbeat_ts, 0),
        rssMb: sanitizeNumber(cached.rss_mb, 0),
        restartTimer: null
      });
    }
  }

  async startAll() {
    for (const agent of this.agents) {
      if (agent.enabled === false) {
        this.logger.info("子代理已按配置禁用", { agent: agent.name });
        continue;
      }
      this._spawnAgent(agent.name, "start");
    }
  }

  _spawnAgent(agentName, reason) {
    if (this.isShuttingDown) {
      return;
    }
    const state = this.children.get(agentName);
    if (!state) {
      return;
    }
    if (state.process) {
      return;
    }

    const maxMemoryMb = 48;
    const child = fork(state.spec.modulePath, state.spec.args ?? [], {
      cwd: this.projectRoot,
      stdio: ["ignore", "inherit", "inherit", "ipc"],
      execArgv: [`--max-old-space-size=${maxMemoryMb}`],
      env: {
        ...process.env,
        MONITOR_GATEWAY_PID: String(process.pid),
        MONITOR_AGENT_NAME: state.spec.name,
        MONITOR_CONFIG: JSON.stringify(state.spec.config ?? {})
      }
    });

    state.process = child;
    state.pid = child.pid;
    state.lastStartTs = nowMs();
    this.logger.info("子代理已启动", {
      agent: state.spec.name,
      pid: child.pid,
      reason
    });

    child.on("message", (message) => {
      this._handleChildMessage(state, message);
    });

    child.on("exit", (code, signal) => {
      this._handleChildExit(state, code, signal);
    });

    child.on("error", (error) => {
      this.logger.error("子进程异常", {
        agent: state.spec.name,
        error: error.message
      });
    });
  }

  async _handleChildMessage(state, message) {
    if (!message || typeof message !== "object") {
      return;
    }
    if (message.type === "status_response") {
      this._handleStatusResponse(state.spec.name, message);
      return;
    }
    if (isAckMessage(message)) {
      return;
    }
    const validation = validateIpcEvent(message);
    if (!validation.ok) {
      this.logger.warn("收到无效的子代理事件", {
        agent: state.spec.name,
        reason: validation.reason
      });
      return;
    }

    if (message.type === "heartbeat") {
      state.lastHeartbeatTs = sanitizeNumber(message.ts, nowMs());
      state.rssMb = sanitizeNumber(message.data?.rss_mb, state.rssMb);
    }

    try {
      await this.onEvent({
        agentName: state.spec.name,
        event: message
      });
      if (state.process?.connected) {
        state.process.send(createAck(message.event_id, true));
      }
    } catch (error) {
      this.logger.error("转发子代理事件失败", {
        agent: state.spec.name,
        error: error.message
      });
      if (state.process?.connected) {
        state.process.send(createAck(message.event_id, false, error.message));
      }
    }
  }

  _handleStatusResponse(agentName, message) {
    const requestId = typeof message.request_id === "string" ? message.request_id : "";
    if (!requestId) {
      return;
    }
    const pending = this.pendingStatusRequests.get(requestId);
    if (!pending) {
      return;
    }
    if (pending.agentName !== agentName) {
      this.logger.warn("状态响应与子代理不匹配", {
        expected: pending.agentName,
        received: agentName,
        request_id: requestId
      });
      return;
    }
    this.pendingStatusRequests.delete(requestId);
    clearTimeout(pending.timer);
    pending.resolve({
      agent: agentName,
      ok: true,
      timed_out: false,
      ts: sanitizeNumber(message.ts, nowMs()),
      data: message.data && typeof message.data === "object" ? message.data : {}
    });
  }

  _resolvePendingStatusForAgent(agentName, reason) {
    for (const [requestId, pending] of this.pendingStatusRequests.entries()) {
      if (pending.agentName !== agentName) {
        continue;
      }
      this.pendingStatusRequests.delete(requestId);
      clearTimeout(pending.timer);
      pending.resolve({
        agent: agentName,
        ok: false,
        timed_out: false,
        error: reason,
        ts: nowMs(),
        data: {}
      });
    }
  }

  _handleChildExit(state, code, signal) {
    const agentName = state.spec.name;
    const exitedPid = state.pid;
    state.process = null;
    state.pid = null;
    this._resolvePendingStatusForAgent(agentName, "agent_exited");

    this.logger.warn("子代理已退出", {
      agent: agentName,
      pid: exitedPid,
      code,
      signal,
      shutdown: this.isShuttingDown
    });

    if (this.isShuttingDown) {
      return;
    }

    this._recordRestart(state);
    const delay = state.nextBackoffMs;
    state.nextBackoffMs = Math.min(state.nextBackoffMs * 2, 60000);

    const timer = setTimeout(() => {
      state.restartTimer = null;
      this._spawnAgent(agentName, "restart");
    }, delay);
    timer.unref();
    state.restartTimer = timer;

    this.logger.warn("已安排子代理重启", {
      agent: agentName,
      delay_ms: delay
    });
  }

  _recordRestart(state) {
    const now = nowMs();
    state.restartCount += 1;
    state.restartHistory.push(now);
    const windowStart = now - this.restartWindowMs;
    state.restartHistory = state.restartHistory.filter((item) => item >= windowStart);

    if (state.restartHistory.length > this.restartCriticalThreshold) {
      const event = createIpcEvent({
        source: "child-supervisor",
        type: "alert_candidate",
        level: "critical",
        rule_id: "child_restart_storm",
        resource_id: state.spec.name,
        title: "子代理重启过多",
        message: `${state.spec.name} 在 10 分钟内重启超过 ${this.restartCriticalThreshold} 次`,
        data: {
          restart_count_10m: state.restartHistory.length,
          threshold: this.restartCriticalThreshold,
          window_ms: this.restartWindowMs
        }
      });
      this.onEvent({ agentName: "child-supervisor", event }).catch((error) => {
        this.logger.error("上报重启风暴失败", {
          agent: state.spec.name,
          error: error.message
        });
      });
    }
  }

  inspectChildren(now = nowMs()) {
    const snapshots = [];
    for (const [name, state] of this.children.entries()) {
      const heartbeatAgeMs = state.lastHeartbeatTs > 0 ? now - state.lastHeartbeatTs : null;
      snapshots.push({
        name,
        pid: state.pid,
        running: Boolean(state.process),
        restart_count: state.restartCount,
        restart_history_count_10m: state.restartHistory.filter((item) => item >= now - this.restartWindowMs).length,
        next_backoff_ms: state.nextBackoffMs,
        last_heartbeat_ts: state.lastHeartbeatTs,
        heartbeat_age_ms: heartbeatAgeMs,
        heartbeat_stale: heartbeatAgeMs != null ? heartbeatAgeMs > this.heartbeatTimeoutMs : true,
        rss_mb: state.rssMb
      });
    }
    return snapshots;
  }

  async requestStatus(agentName, timeoutMs = 5000) {
    const state = this.children.get(agentName);
    if (!state) {
      return {
        agent: agentName,
        ok: false,
        timed_out: false,
        error: "agent_not_found",
        ts: nowMs(),
        data: {}
      };
    }
    if (!state.process || !state.process.connected) {
      return {
        agent: agentName,
        ok: false,
        timed_out: false,
        error: "agent_not_running",
        ts: nowMs(),
        data: {
          running: false
        }
      };
    }

    const requestId = buildStatusRequestId();
    return new Promise((resolve) => {
      const timeout = setTimeout(() => {
        this.pendingStatusRequests.delete(requestId);
        resolve({
          agent: agentName,
          ok: false,
          timed_out: true,
          error: "status_request_timeout",
          ts: nowMs(),
          data: {}
        });
      }, sanitizeNumber(timeoutMs, 5000));
      timeout.unref();

      this.pendingStatusRequests.set(requestId, {
        agentName,
        timer: timeout,
        resolve
      });

      try {
        state.process.send({
          type: "status_request",
          request_id: requestId,
          ts: nowMs()
        });
      } catch (error) {
        this.pendingStatusRequests.delete(requestId);
        clearTimeout(timeout);
        resolve({
          agent: agentName,
          ok: false,
          timed_out: false,
          error: error instanceof Error ? error.message : String(error),
          ts: nowMs(),
          data: {}
        });
      }
    });
  }

  async collectStatuses(timeoutMs = 5000) {
    const tasks = [];
    for (const [agentName] of this.children.entries()) {
      tasks.push(this.requestStatus(agentName, timeoutMs));
    }
    return Promise.all(tasks);
  }

  getSerializableState() {
    const data = {};
    for (const [name, state] of this.children.entries()) {
      data[name] = {
        restart_count: state.restartCount,
        restart_history: state.restartHistory,
        next_backoff_ms: state.nextBackoffMs,
        last_heartbeat_ts: state.lastHeartbeatTs,
        rss_mb: state.rssMb
      };
    }
    return data;
  }

  async stopAll() {
    this.isShuttingDown = true;
    const waiters = [];

    for (const state of this.children.values()) {
      if (state.restartTimer) {
        clearTimeout(state.restartTimer);
        state.restartTimer = null;
      }
      if (!state.process) {
        continue;
      }
      const child = state.process;
      const agentName = state.spec.name;
      this._resolvePendingStatusForAgent(agentName, "gateway_shutdown");
      waiters.push(
        new Promise((resolve) => {
          const timeout = setTimeout(() => {
            if (!child.killed) {
              child.kill("SIGKILL");
            }
            resolve();
          }, 5000);
          timeout.unref();

          child.once("exit", () => {
            clearTimeout(timeout);
            resolve();
          });

          if (child.connected) {
            child.send({ type: "shutdown" });
          }
          child.kill("SIGTERM");

          this.logger.info("正在停止子进程", { agent: agentName, pid: child.pid });
        })
      );
    }

    await Promise.all(waiters);
  }
}
