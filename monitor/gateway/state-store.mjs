import { dirname } from "node:path";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

export class StateStore {
  constructor(filePath, logger) {
    this.filePath = filePath;
    this.logger = logger;
    this.state = {};
    this._saveQueue = Promise.resolve();
  }

  async load(defaultState = {}) {
    await mkdir(dirname(this.filePath), { recursive: true });
    try {
      const text = await readFile(this.filePath, "utf8");
      const parsed = JSON.parse(text);
      if (!parsed || typeof parsed !== "object") {
        throw new Error("state file is not an object");
      }
      this.state = parsed;
      this.logger.info("state loaded", { file: this.filePath });
      return this.state;
    } catch (error) {
      if (error?.code !== "ENOENT") {
        this.logger.warn("state load fallback to defaults", { file: this.filePath, error: error.message });
      } else {
        this.logger.info("state file not found, using defaults", { file: this.filePath });
      }
      this.state = cloneJson(defaultState);
      return this.state;
    }
  }

  getState() {
    return this.state;
  }

  setSection(key, value) {
    this.state[key] = value;
  }

  saveNow() {
    this._saveQueue = this._saveQueue.then(async () => {
      const payload = {
        ...this.state,
        updated_at: new Date().toISOString()
      };
      const tempFile = `${this.filePath}.tmp`;
      await writeFile(tempFile, JSON.stringify(payload, null, 2), "utf8");
      await rename(tempFile, this.filePath);
    });
    return this._saveQueue.catch((error) => {
      this.logger.error("state save failed", { file: this.filePath, error: error.message });
      throw error;
    });
  }
}
