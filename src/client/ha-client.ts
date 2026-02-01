import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';
import * as fs from 'fs';
import { toAny, fromAny, AnyValue } from './converter';

const PROTO_PATH = path.join(__dirname, '../../proto/sql.proto');

export interface HAClientOptions {
  url: string;
  token?: string;
  enableSSL?: boolean;
  timeout?: number;
}

export interface ExecutionResult {
  columns: string[];
  rows: unknown[][];
  rowsAffected?: number;
}

export interface NamedValue {
  name?: string;
  ordinal: number;
  value: AnyValue;
}

enum QueryType {
  QUERY_TYPE_UNSPECIFIED = 0,
  QUERY_TYPE_EXEC_QUERY = 1,
  QUERY_TYPE_EXEC_UPDATE = 2,
}

interface QueryRequest {
  replication_id: string;
  sql: string;
  type: QueryType;
  params: NamedValue[];
}

interface QueryResponse {
  result_set?: {
    columns: string[];
    rows: Array<{ values: AnyValue[] }>;
  };
  rows_affected: number;
  txseq: number;
  error: string;
}

interface DownloadResponse {
  data: Buffer;
}

interface ReplicationIDsResponse {
  replication_id: string[];
}

export class HAClient {
  private channel: grpc.Channel | null = null;
  private client: any;
  private requestStream: grpc.ClientDuplexStream<QueryRequest, QueryResponse> | null = null;
  private responsePromise: { resolve: (value: QueryResponse) => void; reject: (err: Error) => void } | null = null;
  private replicationID: string;
  private txseq: number = 0;
  private timeout: number;
  private token: string;
  private enableSSL: boolean;

  constructor(options: HAClientOptions) {
    const url = new URL(options.url.replace('litesql://', 'http://'));
    this.replicationID = url.pathname.replace(/^\//, '');
    this.timeout = options.timeout ?? 30;
    this.token = options.token ?? '';
    this.enableSSL = options.enableSSL ?? false;

    const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
      keepCase: false,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
    });

    const protoDescriptor = grpc.loadPackageDefinition(packageDefinition) as any;
    const DatabaseService = protoDescriptor.sql.v1.DatabaseService;

    const credentials = this.enableSSL
      ? grpc.credentials.createSsl()
      : grpc.credentials.createInsecure();

    const address = `${url.hostname}:${url.port || 8080}`;
    this.client = new DatabaseService(address, credentials);

    this.initQueryStream();
  }

  private initQueryStream(): void {
    const metadata = new grpc.Metadata();
    if (this.token) {
      metadata.set('authorization', `Bearer ${this.token}`);
    }

    this.requestStream = this.client.Query(metadata);

    this.requestStream!.on('data', (response: QueryResponse) => {
      if (response.txseq > 0) {
        this.txseq = response.txseq;
      }
      if (this.responsePromise) {
        this.responsePromise.resolve(response);
        this.responsePromise = null;
      }
    });

    this.requestStream!.on('error', (err: Error) => {
      if (this.responsePromise) {
        this.responsePromise.reject(err);
        this.responsePromise = null;
      }
    });

    this.requestStream!.on('end', () => {
      this.requestStream = null;
    });
  }

  private async send(
    sql: string,
    parameters: Map<string | number, unknown> | Record<string | number, unknown> | null,
    type: QueryType
  ): Promise<QueryResponse> {
    if (!this.requestStream) {
      this.initQueryStream();
    }

    const params: NamedValue[] = [];
    if (parameters) {
      const entries = parameters instanceof Map ? parameters.entries() : Object.entries(parameters);
      let ordinal = 1;
      for (const [key, value] of entries) {
        const isIndexed = typeof key === 'number';
        params.push({
          name: isIndexed ? undefined : String(key),
          ordinal: isIndexed ? key : ordinal,
          value: toAny(value),
        });
        ordinal++;
      }
    }

    const request: QueryRequest = {
      replication_id: this.replicationID,
      sql,
      type,
      params,
    };

    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        reject(new Error(`Query timeout after ${this.timeout} seconds`));
      }, this.timeout * 1000);

      this.responsePromise = {
        resolve: (response) => {
          clearTimeout(timeoutId);
          resolve(response);
        },
        reject: (err) => {
          clearTimeout(timeoutId);
          reject(err);
        },
      };

      this.requestStream!.write(request);
    });
  }

  /**
   * Execute a SELECT query and return results
   */
  async executeQuery(
    sql: string,
    parameters?: Map<string | number, unknown> | Record<string | number, unknown>
  ): Promise<ExecutionResult> {
    const response = await this.send(sql, parameters ?? null, QueryType.QUERY_TYPE_EXEC_QUERY);

    if (response.error) {
      throw new Error(response.error);
    }

    const columns = response.result_set?.columns ?? [];
    const rows: unknown[][] = [];

    if (response.result_set?.rows) {
      for (const row of response.result_set.rows) {
        const rowData: unknown[] = [];
        for (const value of row.values) {
          rowData.push(fromAny(value));
        }
        rows.push(rowData);
      }
    }

    return { columns, rows };
  }

  /**
   * Execute an INSERT/UPDATE/DELETE statement
   */
  async executeUpdate(
    sql: string,
    parameters?: Map<string | number, unknown> | Record<string | number, unknown>
  ): Promise<number> {
    const response = await this.send(sql, parameters ?? null, QueryType.QUERY_TYPE_EXEC_UPDATE);

    if (response.error) {
      throw new Error(response.error);
    }

    return response.rows_affected;
  }

  /**
   * Execute any SQL statement
   */
  async execute(
    sql: string,
    parameters?: Map<string | number, unknown> | Record<string | number, unknown>
  ): Promise<ExecutionResult> {
    const response = await this.send(sql, parameters ?? null, QueryType.QUERY_TYPE_UNSPECIFIED);

    if (response.error) {
      throw new Error(response.error);
    }

    if (!response.result_set || response.result_set.columns.length === 0) {
      return { columns: [], rows: [], rowsAffected: response.rows_affected };
    }

    const columns = response.result_set.columns;
    const rows: unknown[][] = [];

    for (const row of response.result_set.rows) {
      const rowData: unknown[] = [];
      for (const value of row.values) {
        rowData.push(fromAny(value));
      }
      rows.push(rowData);
    }

    return { columns, rows, rowsAffected: response.rows_affected };
  }

  /**
   * Download a replica database file
   */
  async downloadReplica(dir: string, replicationID: string, override = false): Promise<void> {
    const filePath = path.join(dir, replicationID);
    if (!override && fs.existsSync(filePath)) {
      return;
    }

    const metadata = new grpc.Metadata();
    if (this.token) {
      metadata.set('authorization', `Bearer ${this.token}`);
    }

    return new Promise((resolve, reject) => {
      const call = this.client.Download({ replication_id: replicationID }, metadata);
      const chunks: Buffer[] = [];

      call.on('data', (response: DownloadResponse) => {
        chunks.push(response.data);
      });

      call.on('error', (err: Error) => {
        reject(err);
      });

      call.on('end', () => {
        fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(filePath, Buffer.concat(chunks));
        resolve();
      });
    });
  }

  /**
   * Download all replica database files
   */
  async downloadAllReplicas(dir: string, override = false): Promise<void> {
    const ids = await this.getReplicationIDs();
    for (const id of ids) {
      await this.downloadReplica(dir, id, override);
    }
  }

  /**
   * Get all available replication IDs
   */
  async getReplicationIDs(): Promise<string[]> {
    const metadata = new grpc.Metadata();
    if (this.token) {
      metadata.set('authorization', `Bearer ${this.token}`);
    }

    return new Promise((resolve, reject) => {
      this.client.ReplicationIDs({}, metadata, (err: Error | null, response: ReplicationIDsResponse) => {
        if (err) {
          reject(err);
        } else {
          resolve(response.replication_id || []);
        }
      });
    });
  }

  getReplicationID(): string {
    return this.replicationID;
  }

  setReplicationID(id: string): void {
    this.replicationID = id;
  }

  getTxseq(): number {
    return this.txseq;
  }

  close(): void {
    if (this.requestStream) {
      this.requestStream.end();
      this.requestStream = null;
    }
    if (this.client) {
      this.client.close();
    }
  }
}
