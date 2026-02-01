import { HAConnection, HAConnectionOptions } from './ha-connection';
import { EmbeddedReplicasManager } from './client/embedded-replicas';
import { HAClient } from './client/ha-client';

export interface HADataSourceOptions {
  url: string;
  password?: string;
  enableSSL?: boolean;
  timeout?: number;
  loginTimeout?: number;
  embeddedReplicasDir?: string;
  replicationUrl?: string;
  replicationStream?: string;
  replicationDurable?: string;
}

/**
 * Data source for managing HA database connections
 */
export class HADataSource {
  private url: string = '';
  private password: string = '';
  private enableSSL: boolean = false;
  private timeout: number = 30;
  private loginTimeout: number = 30;
  private embeddedReplicasDir?: string;
  private replicationUrl?: string;
  private replicationStream?: string;
  private replicationDurable?: string;

  constructor(options?: HADataSourceOptions) {
    if (options) {
      this.url = options.url;
      this.password = options.password ?? '';
      this.enableSSL = options.enableSSL ?? false;
      this.timeout = options.timeout ?? 30;
      this.loginTimeout = options.loginTimeout ?? 30;
      this.embeddedReplicasDir = options.embeddedReplicasDir;
      this.replicationUrl = options.replicationUrl;
      this.replicationStream = options.replicationStream;
      this.replicationDurable = options.replicationDurable;
    }
  }

  /**
   * Get a connection from the data source
   */
  async getConnection(): Promise<HAConnection> {
    // Initialize embedded replicas if configured
    if (this.embeddedReplicasDir && this.replicationUrl && this.replicationDurable) {
      const manager = EmbeddedReplicasManager.getInstance();
      await manager.load({
        dir: this.embeddedReplicasDir,
        natsUrl: this.replicationUrl,
        stream: this.replicationStream ?? 'ha',
        durable: this.replicationDurable,
      });
    }

    const options: HAConnectionOptions = {
      url: this.url,
      token: this.password,
      enableSSL: this.enableSSL,
      timeout: this.timeout,
      embeddedReplicasDir: this.embeddedReplicasDir,
      replicationUrl: this.replicationUrl,
      replicationStream: this.replicationStream,
      replicationDurable: this.replicationDurable,
    };

    return new HAConnection(options);
  }

  /**
   * Download all replicas from the HA server
   */
  async downloadReplicas(dir: string, override = false): Promise<void> {
    const client = new HAClient({
      url: this.url,
      token: this.password,
      enableSSL: this.enableSSL,
      timeout: this.timeout,
    });

    try {
      await client.downloadAllReplicas(dir, override);
    } finally {
      client.close();
    }
  }

  // Getters and setters

  getUrl(): string {
    return this.url;
  }

  setUrl(url: string): void {
    this.url = url;
  }

  getPassword(): string {
    return this.password;
  }

  setPassword(password: string): void {
    this.password = password;
  }

  isEnableSSL(): boolean {
    return this.enableSSL;
  }

  setEnableSSL(enable: boolean): void {
    this.enableSSL = enable;
  }

  getTimeout(): number {
    return this.timeout;
  }

  setTimeout(timeout: number): void {
    this.timeout = timeout;
  }

  getLoginTimeout(): number {
    return this.loginTimeout;
  }

  setLoginTimeout(loginTimeout: number): void {
    this.loginTimeout = loginTimeout;
  }

  getEmbeddedReplicasDir(): string | undefined {
    return this.embeddedReplicasDir;
  }

  setEmbeddedReplicasDir(dir: string): void {
    this.embeddedReplicasDir = dir;
  }

  getReplicationUrl(): string | undefined {
    return this.replicationUrl;
  }

  setReplicationUrl(url: string): void {
    this.replicationUrl = url;
  }

  getReplicationStream(): string | undefined {
    return this.replicationStream;
  }

  setReplicationStream(stream: string): void {
    this.replicationStream = stream;
  }

  getReplicationDurable(): string | undefined {
    return this.replicationDurable;
  }

  setReplicationDurable(durable: string): void {
    this.replicationDurable = durable;
  }
}
