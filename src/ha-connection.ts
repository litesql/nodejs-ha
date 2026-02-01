import { HAClient, ExecutionResult, HAClientOptions } from './client/ha-client';
import { EmbeddedReplicasManager } from './client/embedded-replicas';
import Database, { Database as DatabaseType } from 'better-sqlite3';

export interface HAConnectionOptions extends HAClientOptions {
  embeddedReplicasDir?: string;
  replicationUrl?: string;
  replicationStream?: string;
  replicationDurable?: string;
}

/**
 * Represents a connection to the HA database
 */
export class HAConnection {
  private client: HAClient;
  private embeddedReplica: DatabaseType | null = null;
  private replicasManager: EmbeddedReplicasManager | null = null;
  private closed = false;
  private autoCommit = true;
  private readOnly = false;

  constructor(options: HAConnectionOptions) {
    this.client = new HAClient(options);

    // Set up embedded replicas if configured
    if (options.embeddedReplicasDir && options.replicationUrl) {
      this.replicasManager = EmbeddedReplicasManager.getInstance();
      this.embeddedReplica = this.replicasManager.createConnection(this.client.getReplicationID());
    }
  }

  /**
   * Execute a SELECT query
   */
  async query(sql: string, params?: Record<string, unknown>): Promise<ExecutionResult> {
    // Use embedded replica for read queries if available and up-to-date
    if (
      this.embeddedReplica &&
      this.replicasManager &&
      this.isSelectQuery(sql) &&
      this.replicasManager.isReplicaUpdated(this.client.getReplicationID(), this.client.getTxseq())
    ) {
      return this.executeOnReplica(sql, params);
    }

    return this.client.executeQuery(sql, params);
  }

  /**
   * Execute an INSERT/UPDATE/DELETE statement
   */
  async execute(sql: string, params?: Record<string, unknown>): Promise<number> {
    return this.client.executeUpdate(sql, params);
  }

  /**
   * Execute any SQL statement
   */
  async run(sql: string, params?: Record<string, unknown>): Promise<ExecutionResult> {
    // Use embedded replica for read queries if available and up-to-date
    if (
      this.embeddedReplica &&
      this.replicasManager &&
      this.isSelectQuery(sql) &&
      this.replicasManager.isReplicaUpdated(this.client.getReplicationID(), this.client.getTxseq())
    ) {
      return this.executeOnReplica(sql, params);
    }

    return this.client.execute(sql, params);
  }

  private executeOnReplica(sql: string, params?: Record<string, unknown>): ExecutionResult {
    if (!this.embeddedReplica) {
      throw new Error('No embedded replica available');
    }

    const stmt = this.embeddedReplica.prepare(sql);

    if (params) {
      const rows = stmt.all(params) as Record<string, unknown>[];
      if (rows.length === 0) {
        return { columns: [], rows: [] };
      }
      const columns = Object.keys(rows[0]);
      return {
        columns,
        rows: rows.map((row) => columns.map((col) => row[col])),
      };
    }

    const rows = stmt.all() as Record<string, unknown>[];
    if (rows.length === 0) {
      return { columns: [], rows: [] };
    }
    const columns = Object.keys(rows[0]);
    return {
      columns,
      rows: rows.map((row) => columns.map((col) => row[col])),
    };
  }

  private isSelectQuery(sql: string): boolean {
    const trimmed = sql.trim().toUpperCase();
    return (
      trimmed.startsWith('SELECT') ||
      trimmed.startsWith('PRAGMA') ||
      trimmed.startsWith('EXPLAIN') ||
      trimmed.startsWith('WITH')
    );
  }

  /**
   * Begin a transaction
   */
  async beginTransaction(): Promise<void> {
    await this.client.executeUpdate('BEGIN');
    this.autoCommit = false;
  }

  /**
   * Commit the current transaction
   */
  async commit(): Promise<void> {
    await this.client.executeUpdate('COMMIT');
    this.autoCommit = true;
  }

  /**
   * Rollback the current transaction
   */
  async rollback(): Promise<void> {
    await this.client.executeUpdate('ROLLBACK');
    this.autoCommit = true;
  }

  /**
   * Set auto-commit mode
   */
  async setAutoCommit(autoCommit: boolean): Promise<void> {
    if (autoCommit === this.autoCommit) return;

    if (autoCommit) {
      await this.commit();
    } else {
      await this.client.executeUpdate('BEGIN');
    }
    this.autoCommit = autoCommit;
  }

  getAutoCommit(): boolean {
    return this.autoCommit;
  }

  /**
   * Set read-only mode
   */
  async setReadOnly(readOnly: boolean): Promise<void> {
    const pragma = readOnly ? 'PRAGMA query_only = 1' : 'PRAGMA query_only = 0';
    await this.client.executeUpdate(pragma);
    this.readOnly = readOnly;
  }

  isReadOnly(): boolean {
    return this.readOnly;
  }

  /**
   * Check if the connection is valid
   */
  async isValid(timeout = 5): Promise<boolean> {
    try {
      await this.client.executeQuery('SELECT 1');
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Get the current catalog (database name)
   */
  getCatalog(): string {
    return this.client.getReplicationID();
  }

  /**
   * Set the current catalog (database name)
   */
  async setCatalog(catalog: string): Promise<void> {
    if (!catalog) {
      throw new Error('Catalog cannot be empty');
    }

    this.client.setReplicationID(catalog);

    // Update embedded replica if available
    if (this.replicasManager) {
      if (this.embeddedReplica) {
        this.embeddedReplica.close();
      }
      this.embeddedReplica = this.replicasManager.createConnection(catalog);
    }
  }

  /**
   * Get the underlying HAClient
   */
  getClient(): HAClient {
    return this.client;
  }

  isClosed(): boolean {
    return this.closed;
  }

  /**
   * Close the connection
   */
  async close(): Promise<void> {
    if (this.closed) return;

    this.client.close();

    if (this.embeddedReplica) {
      this.embeddedReplica.close();
      this.embeddedReplica = null;
    }

    this.closed = true;
  }
}
