# logit-logger

Unified logging SDK for the **Logit** self-hosted logging ecosystem. This library contains both the frontend browser log capture client and the backend Express log ingestion/WebSocket server.

🔗 **Quick Links:**
- **NPM Package**: [https://www.npmjs.com/package/logit-logger](https://www.npmjs.com/package/logit-logger)
- **Logit Viewer (Dashboard) Repository**: [https://github.com/Abhi-codex/logit-viewer](https://github.com/Abhi-codex/logit-viewer)
- **GitHub Repository**: [https://github.com/Abhi-codex/logit-logger](https://github.com/Abhi-codex/logit-logger)

---

## Table of Contents
1. [Installation](#installation)
2. [Frontend Integration (Browser Client)](#1-frontend-integration-browser-client)
   - [Initialization Options](#initialization-options)
   - [Logging Methods](#logging-methods)
3. [Backend Integration (Ingest Server)](#2-backend-integration-ingest-server)
   - [Server Options](#server-options)
   - [REST APIs & WebSockets](#rest-apis--websockets)
4. [Apache Flume Integration](#3-apache-flume-integration)
5. [Database Pruning & Indexes](#4-database-pruning--indexes)

---

## Installation

Install the package alongside MongoDB (Mongoose) dependencies:

```bash
npm install logit-logger mongoose express socket.io
```

---

## 1. Frontend Integration (Browser Client)

The browser client intercepts native console utilities and catches unhandled runtime errors, batching and piping them to your server.

### Basic Initialization
Place this at the root entry point of your frontend app (e.g. `main.tsx` or `index.js`):

```typescript
import { LogitClient } from 'logit-logger';

const logit = new LogitClient({
  serverUrl: 'http://localhost:4000',           // Target self-hosted server
  ingestKey: 'your-secret-ingest-token-abc123', // Ingest key match
  appName: 'my-web-app',                        // Service app identifier
  matchId: 'prod-frontend-stream',              // Grouping session or match ID
  captureConsole: true,                         // Hook console.log/info/warn/error
  captureErrors: true,                          // Hook unhandled errors & promise failures
  batchIntervalMs: 3000,                        // Transmit buffer every 3 seconds
  maxBatchSize: 20,                             // Or immediately when 20 logs accumulate
  debug: false
});

// The logger is now running! Any standard console call will be routed to Logit automatically.
console.log('User signed in successfully.');
```

### Initialization Options

| Parameter | Type | Required | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `serverUrl` | `string` | Yes | - | URL of your self-hosted Logit backend server. |
| `ingestKey` | `string` | Yes | - | API key configured on the server to authenticate writes. |
| `appName` | `string` | Yes | - | Identifier for the client app (filters in dashboard). |
| `matchId` | `string` | No | `""` | Optional sub-folder structure to track specific user flows. |
| `captureConsole` | `boolean`| No | `true` | Intercept and mirror standard global window console methods. |
| `captureErrors` | `boolean`| No | `true` | Hook global window error and promise rejection events. |
| `batchIntervalMs`| `number` | No | `5000` | Timer interval to flush logs to the network sink (ms). |
| `maxBatchSize` | `number` | No | `30` | Flush buffer limit triggering immediate transmission. |

### Logging Methods
You can log explicitly with contextual metadata objects:

```typescript
logit.info(message: string, context?: Record<string, any>);
logit.warn(message: string, context?: Record<string, any>);
logit.error(message: string, context?: Record<string, any>);
logit.debug(message: string, context?: Record<string, any>);
```

Example:
```typescript
logit.error('Payment checkout transaction failed', {
  cartId: 'cart_099182',
  itemsCount: 3,
  gatewayErrorCode: 'CARD_DECLINED_502'
});
```

---

## 2. Backend Integration (Ingest Server)

Expose REST ingestion routes, handle live Socket.io clients, and query database buffers directly.

### Basic Initialization
Create a server script (e.g. `server.ts`):

```typescript
import { createLogitServer } from 'logit-logger/server';

const server = createLogitServer({
  username: 'admin',                          // Username to connect via Logit Viewer
  password: 'your-secure-admin-password',     // Password to connect via Logit Viewer
  ingestKey: 'your-secret-ingest-token-abc123', // Ingest key authorized for clients
  mongoUri: 'mongodb://127.0.0.1:27017/logit',// MongoDB Target Database connection
  port: 4000,                                 // Port (runs Express & Socket.io)
  maxLogCount: 10000                          // Queue ceiling (prunes oldest logs)
});
```

### Server Options

| Option | Type | Required | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `username` | `string` | Yes | - | Logit Viewer console login username. |
| `password` | `string` | Yes | - | Logit Viewer console login password. |
| `ingestKey` | `string` | Yes | - | Header validation key (e.g., `x-ingest-key`) expected from clients. |
| `mongoUri` | `string` | Yes | - | Mongo connection URL. Supports cluster URLs. |
| `port` | `number` | No | `4000` | Listening port for the application server. |
| `maxLogCount`| `number` | No | `10000` | Size of the FIFO log rolling database queue. |

### REST APIs & WebSockets

The ingest server exposes the following routes for log piping and viewer queries:

*   `POST /logit/login` - Authenticates console users and returns temporary Bearer JWT tokens.
*   `POST /logit/ingest` - Receives client batch payloads. Expected header format: `Authorization: Bearer <ingestKey>`.
*   `POST /logit/flume` - Receives Apache Flume event batches.
*   `GET /logit/logs` - Admin-only query route to fetch historical logs. Supports query parameters `from`, `to`, `app`, and `level`.
*   `GET /logit/me` - Validates session validity.
*   `Socket.io (Connection)` - Handles dashboard broadcast feeds. Authenticates clients using JWT handshakes.

---

## 3. Apache Flume Integration

Logit works out-of-the-box as an HTTP target for Apache Flume pipelines. The `/logit/flume` route automatically base64-decodes event payload streams.

Configure your Flume agent sink in `flume.conf`:

```ini
# Define agent components
agent.sources = source1
agent.sinks = logit-sink
agent.channels = channel1

# Configure source (example syslog source)
agent.sources.source1.type = syslogudp
agent.sources.source1.port = 5140

# Configure HTTP sink
agent.sinks.logit-sink.type = http
agent.sinks.logit-sink.endpoint = http://localhost:4000/logit/flume
agent.sinks.logit-sink.contentType = application/json
agent.sinks.logit-sink.headers.ingest-key = your-secret-ingest-token-abc123

# Bind source and sink to channel
agent.sources.source1.channels = channel1
agent.sinks.logit-sink.channel = channel1
```

---

## 4. Database Pruning & Indexes

### Pruning Pipeline
Logit prevents database storage exhaustion using a FIFO (First-In-First-Out) capping system:
1. When a log batch is inserted, the server counts the total items in the target database.
2. If `currentCount > maxLogCount`, the excess count is calculated.
3. The server deletes the oldest `excess` records (sorted chronologically by `timestamp`).

### Database Optimization Sinks
Ensure high-throughput dashboard querying by adding a compound index to MongoDB:

```javascript
// Run in your MongoDB console
db.logs.createIndex({ matchId: 1, appName: 1, level: 1, timestamp: -1 });
```
# logit-logger
