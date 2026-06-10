import express from 'express';
import http from 'http';
import { Server as SocketIOServer } from 'socket.io';
import cors from 'cors';
import jwt from 'jsonwebtoken';
import { connectDatabase, LogModel, ILog, pruneLogs } from './db.js';

export interface LogitServerConfig {
  username: string;
  password: string;
  ingestKey: string;
  mongoUri: string;
  redisUri?: string; // Kept for future scaling if needed
  port?: number;
  jwtSecret?: string;
  maxLogCount?: number;
}

export function createLogitServer(config: LogitServerConfig) {
  const app = express();
  const server = http.createServer(app);
  const io = new SocketIOServer(server, {
    cors: {
      origin: '*',
      methods: ['GET', 'POST']
    }
  });

  const port = config.port || 4000;
  const jwtSecret = config.jwtSecret || 'logit-jwt-super-secret-key-12345';
  const maxLogCount = config.maxLogCount || 10000;

  app.use(cors());
  app.use(express.json());

  // Connect to DB asynchronously
  connectDatabase(config.mongoUri).catch((err) => {
    console.error('[Logit Database] MongoDB connection failed:', err);
  });

  // --- Middleware ---
  
  // 1. Verify Ingestion Key
  const verifyIngestKey = (req: express.Request, res: express.Response, next: express.NextFunction) => {
    let token = '';
    const authHeader = req.headers.authorization;
    if (authHeader && authHeader.startsWith('Bearer ')) {
      token = authHeader.substring(7);
    } else if (req.query.token) {
      token = String(req.query.token);
    }

    if (token !== config.ingestKey) {
      return res.status(401).json({ success: false, message: 'Invalid or missing Ingestion Key.' });
    }
    next();
  };

  // 2. Verify JWT (for Viewer Dashboard routes)
  const verifyViewerToken = (req: any, res: express.Response, next: express.NextFunction) => {
    const authHeader = req.headers.authorization;
    let token = '';
    if (authHeader && authHeader.startsWith('Bearer ')) {
      token = authHeader.substring(7);
    }

    if (!token) {
      return res.status(401).json({ success: false, message: 'Authorization token required.' });
    }

    try {
      const decoded = jwt.verify(token, jwtSecret);
      req.user = decoded;
      next();
    } catch (e) {
      return res.status(403).json({ success: false, message: 'Invalid or expired Authorization token.' });
    }
  };

  // --- Ingestion APIs ---

  // Standard JSON logs array batch ingestion
  app.post('/logit/ingest', verifyIngestKey, async (req: express.Request, res: express.Response) => {
    const { logs } = req.body;
    if (!Array.isArray(logs)) {
      return res.status(400).json({ success: false, message: 'Invalid logs payload. Expected array under "logs".' });
    }

    try {
      const savedLogs: ILog[] = [];
      for (const log of logs) {
        const normalizedLog: ILog = {
          id: log.id || `svr-${Date.now()}-${Math.floor(Math.random() * 1000000)}`,
          matchId: log.matchId || log.match_id || 'GENERAL-STREAM',
          timestamp: log.timestamp || Date.now(),
          responseSentAt: log.responseSentAt || Date.now(),
          appName: log.appName || 'NodeApp',
          level: (log.level || 'info').toLowerCase(),
          method: log.method || '',
          url: log.url || '',
          status: log.status !== undefined ? log.status : 200,
          ip: log.ip || req.ip || '',
          content_length: log.content_length !== undefined ? log.content_length : 0,
          response_time: log.response_time !== undefined ? log.response_time : 0,
          message: log.message || '',
          metadata: log.metadata || {}
        };

        const newLog = new LogModel(normalizedLog);
        await newLog.save();
        savedLogs.push(normalizedLog);

        // Stream real-time to active WebSocket clients
        io.to(normalizedLog.matchId).emit('app_log', normalizedLog);
        io.to('master-room').emit('app_log', normalizedLog);
      }

      await pruneLogs(maxLogCount);
      res.status(200).json({ success: true, count: savedLogs.length });
    } catch (err: any) {
      console.error('[Logit Ingest] Error saving logs:', err);
      res.status(500).json({ success: false, error: err.message });
    }
  });

  // Apache Flume HTTP Ingestion Source endpoint
  app.post('/logit/flume', async (req: express.Request, res: express.Response) => {
    const flumeEvents = req.body;
    if (!Array.isArray(flumeEvents)) {
      return res.status(400).json({ success: false, message: 'Invalid Flume payload. Expected array of events.' });
    }

    try {
      const savedLogs: ILog[] = [];
      for (const event of flumeEvents) {
        const headers = event.headers || {};
        let rawBody = event.body || '';

        const isBase64 = (str: string) => {
          if (!str || str.trim() === '') return false;
          try {
            return Buffer.from(str, 'base64').toString('base64') === str.trim();
          } catch (e) {
            return false;
          }
        };
        
        if (isBase64(rawBody)) {
          rawBody = Buffer.from(rawBody, 'base64').toString('utf-8');
        }

        const logTimestamp = headers.timestamp ? Number(headers.timestamp) : Date.now();
        const appName = headers.appName || headers.application || 'FlumeApp';
        const level = (headers.level || 'info').toLowerCase();
        const matchId = headers.matchId || headers.match_id || 'FLUME-STREAM';

        const normalizedLog: ILog = {
          id: `flume-${Date.now()}-${Math.floor(Math.random() * 1000000)}`,
          matchId,
          timestamp: logTimestamp,
          responseSentAt: Date.now(),
          appName,
          level,
          method: headers.method || '',
          url: headers.url || '',
          status: headers.status !== undefined ? Number(headers.status) : 200,
          ip: headers.ip || req.ip || '',
          content_length: headers.content_length !== undefined ? Number(headers.content_length) : 0,
          response_time: headers.response_time !== undefined ? Number(headers.response_time) : 0,
          message: rawBody,
          metadata: headers
        };

        const newLog = new LogModel(normalizedLog);
        await newLog.save();
        savedLogs.push(normalizedLog);

        // Stream real-time to active WebSocket clients
        io.to(normalizedLog.matchId).emit('app_log', normalizedLog);
        io.to('master-room').emit('app_log', normalizedLog);
      }

      await pruneLogs(maxLogCount);
      res.status(200).json({ success: true, count: savedLogs.length });
    } catch (err: any) {
      console.error('[Logit Ingest Flume] Error:', err);
      res.status(500).json({ success: false, error: err.message });
    }
  });

  // --- Viewer Platform APIs ---

  // Authenticate user dashboard
  app.post('/logit/login', (req: express.Request, res: express.Response) => {
    const { username, password } = req.body;
    if (username === config.username && password === config.password) {
      const token = jwt.sign({ username }, jwtSecret, { expiresIn: '7d' });
      return res.status(200).json({ success: true, token, user: { name: username, email: `${username}@logit.selfhost` } });
    }
    return res.status(401).json({ success: false, message: 'Invalid username or password.' });
  });

  // Check token session validity
  app.get('/logit/me', verifyViewerToken, (req: any, res: express.Response) => {
    res.status(200).json({ success: true, email: `${req.user.username}@logit.selfhost`, name: req.user.username });
  });

  // Get administrative operators list (mimics original API hierarchy paths)
  app.get('/logit/admin_plane_user', verifyViewerToken, async (req: express.Request, res: express.Response) => {
    try {
      const distinctUsers = await LogModel.distinct('metadata.userEmail');
      const formatted = distinctUsers.map((email, i) => ({
        id: i + 1,
        name: email.split('@')[0],
        email: email
      }));
      res.status(200).json([{ status: true, data: formatted }, 200]);
    } catch (e: any) {
      res.status(500).json({ success: false, error: e.message });
    }
  });

  // Get user matches list (mimics original path: /khel/user_match_list)
  app.post('/logit/user_match_list', verifyViewerToken, async (req: express.Request, res: express.Response) => {
    const { user_id } = req.body;
    try {
      const distinctMatches = await LogModel.distinct('matchId');
      const matches = distinctMatches.map((matchId, i) => ({
        match_id: matchId,
        matchId: matchId,
        total_videos: 0,
        user_id: user_id || 1,
        is_active: true
      }));
      res.status(200).json([{ status: true, data: matches }, 200]);
    } catch (e: any) {
      res.status(500).json({ success: false, error: e.message });
    }
  });

  // Query historical logs
  app.get('/logit/logs', verifyViewerToken, async (req: express.Request, res: express.Response) => {
    const { from, to, level, appName, matchId, limit } = req.query;
    const queryFilters: any = {};

    if (from || to) {
      queryFilters.timestamp = {};
      if (from) queryFilters.timestamp.$gte = Number(from);
      if (to) queryFilters.timestamp.$lte = Number(to);
    }

    if (level) queryFilters.level = String(level).toLowerCase();
    if (appName) queryFilters.appName = String(appName);
    if (matchId) queryFilters.matchId = String(matchId);

    const logsLimit = limit ? Number(limit) : 500;

    try {
      const logs = await LogModel.find(queryFilters)
        .sort({ timestamp: -1 })
        .limit(logsLimit)
        .exec();

      res.status(200).json(logs);
    } catch (err: any) {
      console.error('[Logit Server] Error fetching historical logs:', err);
      res.status(500).json({ success: false, error: err.message });
    }
  });

  // --- WebSocket Authentication ---
  io.use((socket: any, next) => {
    const token = socket.handshake.auth?.token;
    if (!token) {
      return next(new Error('Authentication token required'));
    }

    try {
      jwt.verify(token, jwtSecret);
      next();
    } catch (e) {
      next(new Error('Invalid socket connection token'));
    }
  });

  io.on('connection', (socket: any) => {
    console.log('[Logit WebSockets] Client connected:', socket.id);

    const userQuery = socket.handshake.query?.user;
    const masterQuery = socket.handshake.query?.master;

    if (masterQuery === 'true') {
      socket.join('master-room');
      console.log(`[Logit WebSockets] Client ${socket.id} joined master stream.`);
    } else if (userQuery) {
      socket.join(userQuery);
      console.log(`[Logit WebSockets] Client ${socket.id} joined stream room: ${userQuery}`);
    }

    socket.on('disconnect', () => {
      console.log('[Logit WebSockets] Client disconnected:', socket.id);
    });
  });

  server.listen(port, () => {
    console.log(`\n🚀 [Logit Server] Running on http://localhost:${port}`);
    console.log(`👉 Logs Ingest Endpoint: http://localhost:${port}/logit/ingest`);
    console.log(`👉 Flume Ingest Endpoint: http://localhost:${port}/logit/flume`);
  });

  return { app, server, io };
}
export { pruneLogs } from './db.js';
