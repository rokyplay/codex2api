const LEVEL_PRIORITY = Object.freeze({
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
  critical: 50
});

function normalizeLevel(level) {
  if (typeof level !== "string") {
    return "info";
  }
  const normalized = level.toLowerCase();
  return LEVEL_PRIORITY[normalized] ? normalized : "info";
}

function normalizeMeta(meta) {
  if (meta == null) {
    return {};
  }
  if (meta instanceof Error) {
    return {
      error: {
        name: meta.name,
        message: meta.message,
        stack: meta.stack
      }
    };
  }
  if (typeof meta !== "object" || Array.isArray(meta)) {
    return { value: meta };
  }
  return meta;
}

export function shouldLog(level, minLevel) {
  const normalizedLevel = normalizeLevel(level);
  const normalizedMinLevel = normalizeLevel(minLevel);
  return LEVEL_PRIORITY[normalizedLevel] >= LEVEL_PRIORITY[normalizedMinLevel];
}

export function createLogger(label, options = {}) {
  const minLevel = normalizeLevel(options.level ?? "info");

  function emit(level, message, meta) {
    if (!shouldLog(level, minLevel)) {
      return;
    }
    const payload = {
      ts: new Date().toISOString(),
      level: normalizeLevel(level),
      label,
      message: String(message ?? ""),
      ...normalizeMeta(meta)
    };
    const output = JSON.stringify(payload);
    if (LEVEL_PRIORITY[payload.level] >= LEVEL_PRIORITY.error) {
      console.error(output);
      return;
    }
    console.log(output);
  }

  return {
    label,
    level: minLevel,
    debug(message, meta) {
      emit("debug", message, meta);
    },
    info(message, meta) {
      emit("info", message, meta);
    },
    warn(message, meta) {
      emit("warn", message, meta);
    },
    error(message, meta) {
      emit("error", message, meta);
    },
    critical(message, meta) {
      emit("critical", message, meta);
    },
    child(suffix, childOptions = {}) {
      const childLabel = `${label}/${suffix}`;
      return createLogger(childLabel, {
        level: childOptions.level ?? minLevel
      });
    }
  };
}

export const LOG_LEVELS = Object.freeze(Object.keys(LEVEL_PRIORITY));
