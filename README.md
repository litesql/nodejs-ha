# @litesql/ha

A high-performance Node.js client for [SQLite HA](https://github.com/litesql/ha) with built-in replication and failover support.

## Overview

`@litesql/ha` is a Node.js client that brings enterprise-grade high availability to SQLite databases. It seamlessly integrates with Node.js applications while providing automatic failover, embedded replicas for read optimization, and replication support through NATS messaging.

## Features

- **High Availability**: Automatic failover and connection recovery for uninterrupted database access
- **Embedded Replicas**: Download and query read-only replicas locally for improved read performance
- **Replication Support**: Real-time synchronization using NATS messaging
- **TypeScript Support**: Full TypeScript definitions included
- **Lightweight**: Minimal overhead with efficient resource usage

## Installation

```bash
npm install @litesql/ha
```

## Quick Start

### 1. Start the HA Server

[Download the HA server](https://litesql.github.io/ha/downloads/) compatible with your operating system.

```sh
ha mydatabase.sqlite
```

### 2. Create a DataSource

```typescript
import { HADataSource } from '@litesql/ha';

const dataSource = new HADataSource({
  url: 'litesql://localhost:8080',
});
```

### 3. Execute Queries

```typescript
const connection = await dataSource.getConnection();

// SELECT query
const result = await connection.query('SELECT * FROM users');
console.log(result.columns); // ['id', 'name', 'email']
console.log(result.rows);    // [[1, 'Alice', 'alice@example.com'], ...]

// INSERT/UPDATE/DELETE
const rowsAffected = await connection.execute(
  'INSERT INTO users (name, email) VALUES (?, ?)',
  { 1: 'Bob', 2: 'bob@example.com' }
);

await connection.close();
```

## Advanced Configuration

### Using Embedded Replicas for Read Optimization

Embedded replicas allow you to download and query read-only copies of your database locally, improving read performance. All write operations are automatically routed to the HA server.

```typescript
import { HADataSource, HAClient } from '@litesql/ha';

const replicaDir = '/path/to/replicas';

// Download replicas from the HA server
const client = new HAClient({
  url: 'http://localhost:8080',
  token: 'your-secret-key',
});
await client.downloadAllReplicas(replicaDir, true);
client.close();

// Configure the DataSource to use embedded replicas
const dataSource = new HADataSource({
  url: 'litesql://localhost:8080',
  embeddedReplicasDir: replicaDir,
  replicationUrl: 'nats://localhost:4222',
  replicationDurable: 'unique_replica_name',
});

const connection = await dataSource.getConnection();

// Read queries automatically use local replicas when up-to-date
const result = await connection.query('SELECT * FROM users');
```

**Benefits:**
- Faster read queries by querying local replicas
- Reduced network latency
- Automatic synchronization via NATS
- Write operations still go through the central HA server for consistency

### Connection Options

```typescript
const dataSource = new HADataSource({
  url: 'litesql://localhost:8080',     // HA server URL
  password: 'secret-token',             // Authentication token
  enableSSL: true,                      // Enable SSL/TLS
  timeout: 30,                          // Query timeout in seconds
  loginTimeout: 30,                     // Connection timeout in seconds
  embeddedReplicasDir: '/replicas',     // Local replicas directory
  replicationUrl: 'nats://localhost:4222', // NATS server URL
  replicationStream: 'ha',              // NATS stream name
  replicationDurable: 'my-app',         // Durable consumer name
});
```

### Transactions

```typescript
const connection = await dataSource.getConnection();

await connection.beginTransaction();
try {
  await connection.execute('INSERT INTO users (name) VALUES (?)', { 1: 'Alice' });
  await connection.execute('INSERT INTO users (name) VALUES (?)', { 1: 'Bob' });
  await connection.commit();
} catch (error) {
  await connection.rollback();
  throw error;
}

await connection.close();
```

### Direct Client Usage

For more control, you can use the HAClient directly:

```typescript
import { HAClient } from '@litesql/ha';

const client = new HAClient({
  url: 'litesql://localhost:8080/mydb',
  token: 'secret',
});

// Execute queries
const result = await client.executeQuery('SELECT * FROM users');

// Execute updates
const rowsAffected = await client.executeUpdate('DELETE FROM users WHERE id = ?', { 1: 5 });

// Download replicas
await client.downloadReplica('/replicas', 'mydb', false);

client.close();
```

## API Reference

### HADataSource

| Method | Description |
|--------|-------------|
| `getConnection()` | Get a new connection |
| `downloadReplicas(dir, override?)` | Download all replicas to a directory |
| `setUrl(url)` | Set the server URL |
| `setPassword(password)` | Set authentication token |
| `setEnableSSL(enable)` | Enable/disable SSL |
| `setTimeout(seconds)` | Set query timeout |
| `setEmbeddedReplicasDir(dir)` | Set replicas directory |
| `setReplicationUrl(url)` | Set NATS server URL |
| `setReplicationDurable(name)` | Set durable consumer name |

### HAConnection

| Method | Description |
|--------|-------------|
| `query(sql, params?)` | Execute a SELECT query |
| `execute(sql, params?)` | Execute INSERT/UPDATE/DELETE |
| `run(sql, params?)` | Execute any SQL statement |
| `beginTransaction()` | Start a transaction |
| `commit()` | Commit the transaction |
| `rollback()` | Rollback the transaction |
| `setAutoCommit(autoCommit)` | Set auto-commit mode |
| `setReadOnly(readOnly)` | Set read-only mode |
| `setCatalog(catalog)` | Switch database |
| `isValid(timeout?)` | Check connection validity |
| `close()` | Close the connection |

### HAClient

| Method | Description |
|--------|-------------|
| `executeQuery(sql, params?)` | Execute SELECT query |
| `executeUpdate(sql, params?)` | Execute INSERT/UPDATE/DELETE |
| `execute(sql, params?)` | Execute any statement |
| `downloadReplica(dir, id, override?)` | Download a single replica |
| `downloadAllReplicas(dir, override?)` | Download all replicas |
| `getReplicationIDs()` | Get available database names |
| `close()` | Close the client |

## Troubleshooting

### Connection Issues

If you're unable to connect to the HA server:
- Verify the HA server is running on the specified host and port
- Check network connectivity and firewall rules
- Ensure the URL format is correct: `litesql://hostname:port`

### Replica Synchronization

If replicas are not syncing:
- Verify the NATS server is running at the configured `replicationUrl`
- Check that the `replicationDurable` name is unique across your application instances
- Ensure the HA server has appropriate permissions to write to the replica directory

## License

This project is licensed under the Apache v2 License - see the [LICENSE](LICENSE) file for details.
