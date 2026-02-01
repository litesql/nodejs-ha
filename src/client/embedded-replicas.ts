import Database, { Database as DatabaseType } from 'better-sqlite3';
import * as fs from 'fs';
import * as path from 'path';
import { connect, NatsConnection, JetStreamClient, ConsumerMessages } from 'nats';

export interface ReplicaOptions {
  dir: string;
  natsUrl: string;
  stream: string;
  durable: string;
}

interface ReplicaConnection {
  db: DatabaseType;
  txseq: number;
  dsn: string;
}

export class EmbeddedReplicasManager {
  private static instance: EmbeddedReplicasManager | null = null;
  private replicas: Map<string, ReplicaConnection> = new Map();
  private natsConnection: NatsConnection | null = null;
  private jetstream: JetStreamClient | null = null;
  private consumers: Map<string, ConsumerMessages> = new Map();
  private updateInterval: NodeJS.Timeout | null = null;

  private constructor() {}

  static getInstance(): EmbeddedReplicasManager {
    if (!EmbeddedReplicasManager.instance) {
      EmbeddedReplicasManager.instance = new EmbeddedReplicasManager();
    }
    return EmbeddedReplicasManager.instance;
  }

  /**
   * Load replicas from a directory and connect to NATS for replication
   */
  async load(options: ReplicaOptions): Promise<void> {
    const { dir, natsUrl, stream, durable } = options;

    if (!fs.existsSync(dir) || !fs.statSync(dir).isDirectory()) {
      throw new Error(`Invalid directory: ${dir}`);
    }

    // Connect to NATS
    this.natsConnection = await connect({ servers: natsUrl });
    this.jetstream = this.natsConnection.jetstream();

    const files = fs.readdirSync(dir);

    for (const file of files) {
      const filePath = path.join(dir, file);

      if (!fs.statSync(filePath).isFile() || this.replicas.has(file)) {
        continue;
      }

      if (!this.isSqliteFile(filePath)) {
        continue;
      }

      try {
        const db = new Database(filePath, { readonly: false });
        db.pragma('journal_mode = WAL');
        db.pragma('temp_store = MEMORY');
        db.pragma('busy_timeout = 5000');

        // Get initial txseq
        let txseq = 0;
        try {
          const row = db.prepare('SELECT received_seq FROM ha_stats ORDER BY updated_at DESC LIMIT 1').get() as
            | { received_seq: number }
            | undefined;
          if (row) {
            txseq = row.received_seq;
          }
        } catch {
          // ha_stats table might not exist yet
        }

        this.replicas.set(file, {
          db,
          txseq,
          dsn: filePath,
        });

        // Subscribe to replication stream for this replica
        await this.subscribeToReplication(file, stream, durable);
      } catch (err) {
        console.error(`Failed to load replica ${file}:`, err);
      }
    }

    // Start periodic txseq update
    this.startTxseqUpdater();
  }

  private async subscribeToReplication(replicaName: string, stream: string, durable: string): Promise<void> {
    if (!this.jetstream) return;

    const subject = `${stream}.${replicaName.replace(/\./g, '_')}`;

    try {
      const consumer = await this.jetstream.consumers.get(stream, durable);
      const messages = await consumer.consume();

      this.consumers.set(replicaName, messages);

      // Process messages in background
      (async () => {
        for await (const msg of messages) {
          try {
            await this.applyReplicationMessage(replicaName, msg.data);
            msg.ack();
          } catch (err) {
            console.error(`Error applying replication message for ${replicaName}:`, err);
          }
        }
      })();
    } catch (err) {
      console.error(`Failed to subscribe to replication for ${replicaName}:`, err);
    }
  }

  private async applyReplicationMessage(replicaName: string, data: Uint8Array): Promise<void> {
    const replica = this.replicas.get(replicaName);
    if (!replica) return;

    // The replication message format depends on the HA server implementation
    // This is a placeholder for the actual replication logic
    const message = JSON.parse(new TextDecoder().decode(data));

    if (message.sql) {
      replica.db.exec(message.sql);
    }

    if (message.txseq) {
      replica.txseq = message.txseq;
    }
  }

  private startTxseqUpdater(): void {
    if (this.updateInterval) return;

    this.updateInterval = setInterval(() => {
      for (const [name, replica] of this.replicas) {
        try {
          const row = replica.db.prepare('SELECT received_seq FROM ha_stats ORDER BY updated_at DESC LIMIT 1').get() as
            | { received_seq: number }
            | undefined;
          if (row) {
            replica.txseq = row.received_seq;
          }
        } catch {
          // Ignore errors
        }
      }
    }, 5000);
  }

  /**
   * Get a replica connection by database name
   */
  getReplica(dbName: string): ReplicaConnection | undefined {
    if (this.replicas.size === 1 && (!dbName || dbName === '')) {
      return this.replicas.values().next().value;
    }
    return this.replicas.get(dbName);
  }

  /**
   * Create a new connection to a replica
   */
  createConnection(dbName: string): DatabaseType | null {
    const replica = this.getReplica(dbName);
    if (!replica) return null;

    return new Database(replica.dsn, { readonly: true });
  }

  /**
   * Check if a replica is up to date with the given txseq
   */
  isReplicaUpdated(dbName: string, txseq: number): boolean {
    const replica = this.getReplica(dbName);
    if (!replica) return false;
    return replica.txseq >= txseq;
  }

  private isSqliteFile(filePath: string): boolean {
    try {
      const stats = fs.statSync(filePath);
      if (stats.size < 100) return false;

      const fd = fs.openSync(filePath, 'r');
      const buffer = Buffer.alloc(16);
      fs.readSync(fd, buffer, 0, 16, 0);
      fs.closeSync(fd);

      return buffer.toString('utf8').startsWith('SQLite format 3');
    } catch {
      return false;
    }
  }

  /**
   * Stop all replicas and close connections
   */
  async close(): Promise<void> {
    if (this.updateInterval) {
      clearInterval(this.updateInterval);
      this.updateInterval = null;
    }

    for (const [, consumer] of this.consumers) {
      consumer.stop();
    }
    this.consumers.clear();

    for (const [, replica] of this.replicas) {
      replica.db.close();
    }
    this.replicas.clear();

    if (this.natsConnection) {
      await this.natsConnection.close();
      this.natsConnection = null;
    }
  }
}
