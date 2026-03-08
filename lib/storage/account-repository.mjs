/**
 * Account repository abstraction.
 * Concrete implementations must keep field mapping consistent with AccountPool inputs.
 */
export class AccountRepository {
  async init() {
    throw new Error('AccountRepository.init() must be implemented');
  }

  async close() {
    throw new Error('AccountRepository.close() must be implemented');
  }

  async getByEmail(email) {
    throw new Error('AccountRepository.getByEmail(email) must be implemented');
  }

  async getAll(filters) {
    throw new Error('AccountRepository.getAll(filters) must be implemented');
  }

  async getByStatus(status) {
    throw new Error('AccountRepository.getByStatus(status) must be implemented');
  }

  async upsert(account) {
    throw new Error('AccountRepository.upsert(account) must be implemented');
  }

  async upsertBatch(accounts) {
    throw new Error('AccountRepository.upsertBatch(accounts) must be implemented');
  }

  async updateFields(email, fields) {
    throw new Error('AccountRepository.updateFields(email, fields) must be implemented');
  }

  async updateFieldsBatch(items) {
    throw new Error('AccountRepository.updateFieldsBatch(items) must be implemented');
  }

  async appendEvent(event) {
    throw new Error('AccountRepository.appendEvent(event) must be implemented');
  }

  async appendEventBatch(events) {
    throw new Error('AccountRepository.appendEventBatch(events) must be implemented');
  }

  async cleanupEvents(olderThanMs) {
    throw new Error('AccountRepository.cleanupEvents(olderThanMs) must be implemented');
  }

  async delete(email) {
    throw new Error('AccountRepository.delete(email) must be implemented');
  }

  async count(status) {
    throw new Error('AccountRepository.count(status) must be implemented');
  }

  async flush() {
    throw new Error('AccountRepository.flush() must be implemented');
  }
}
