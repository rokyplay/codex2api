export class RingWindow {
  constructor(options = {}) {
    const capacity = Number(options.capacity ?? 4096);
    this.capacity = Number.isFinite(capacity) && capacity > 0 ? Math.floor(capacity) : 4096;
    this.windowMs = Number(options.windowMs ?? 300000);
    this.getTimestamp =
      typeof options.getTimestamp === "function"
        ? options.getTimestamp
        : (item) => Number(item?.ts ?? 0);

    this.buffer = new Array(this.capacity);
    this.start = 0;
    this.length = 0;
  }

  push(item) {
    if (this.length < this.capacity) {
      const index = (this.start + this.length) % this.capacity;
      this.buffer[index] = item;
      this.length += 1;
      return;
    }
    this.buffer[this.start] = item;
    this.start = (this.start + 1) % this.capacity;
  }

  prune(now = Date.now()) {
    const cutoff = now - this.windowMs;
    while (this.length > 0) {
      const item = this.buffer[this.start];
      const ts = this.getTimestamp(item);
      if (!Number.isFinite(ts) || ts >= cutoff) {
        break;
      }
      this.buffer[this.start] = undefined;
      this.start = (this.start + 1) % this.capacity;
      this.length -= 1;
    }
  }

  values(now = Date.now()) {
    this.prune(now);
    const result = [];
    for (let i = 0; i < this.length; i += 1) {
      const index = (this.start + i) % this.capacity;
      result.push(this.buffer[index]);
    }
    return result;
  }

  get size() {
    return this.length;
  }
}
