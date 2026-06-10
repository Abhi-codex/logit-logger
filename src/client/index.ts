export interface LogitClientConfig {
  serverUrl: string;
  ingestKey: string;
  appName?: string;
  matchId?: string;
  batchIntervalMs?: number;
  maxBatchSize?: number;
  captureConsole?: boolean;
  captureErrors?: boolean;
}

export interface LogPayload {
  id: string;
  timestamp: number;
  responseSentAt: number;
  appName: string;
  level: string;
  message: string;
  url: string;
  method?: string;
  status?: string | number;
  ip?: string;
  content_length?: string | number;
  response_time?: number;
  matchId: string;
  [key: string]: any;
}

export class LogitClient {
  private serverUrl: string;
  private ingestKey: string;
  private appName: string;
  private matchId: string;
  private batchIntervalMs: number;
  private maxBatchSize: number;
  private logQueue: LogPayload[] = [];
  private timer: any = null;

  constructor(config: LogitClientConfig) {
    this.serverUrl = config.serverUrl.replace(/\/$/, '');
    this.ingestKey = config.ingestKey;
    this.appName = config.appName || 'BrowserApp';
    this.matchId = config.matchId || 'GENERAL-STREAM';
    this.batchIntervalMs = config.batchIntervalMs || 3000;
    this.maxBatchSize = config.maxBatchSize || 20;

    this.startTimer();

    if (config.captureConsole !== false) {
      this.bindConsole();
    }
    if (config.captureErrors !== false) {
      this.bindErrorListeners();
    }

    // Attempt to flush on page unload
    if (typeof window !== 'undefined') {
      window.addEventListener('beforeunload', () => this.flushSync());
    }
  }

  private startTimer() {
    if (typeof window !== 'undefined') {
      this.timer = setInterval(() => this.flush(), this.batchIntervalMs);
    } else if (typeof global !== 'undefined') {
      this.timer = setInterval(() => this.flush(), this.batchIntervalMs);
    }
  }

  public log(message: string, level: string = 'info', extra: Record<string, any> = {}) {
    const logEntry: LogPayload = {
      ...extra,
      id: `client-${Date.now()}-${Math.floor(Math.random() * 1000000)}`,
      timestamp: Date.now(),
      responseSentAt: Date.now(),
      appName: this.appName,
      level: level.toLowerCase(),
      message,
      url: typeof window !== 'undefined' ? window.location.href : 'server-side',
      matchId: extra.matchId || this.matchId,
    };

    this.logQueue.push(logEntry);

    if (this.logQueue.length >= this.maxBatchSize) {
      this.flush();
    }
  }

  public info(message: string, extra?: Record<string, any>) {
    this.log(message, 'info', extra);
  }

  public warn(message: string, extra?: Record<string, any>) {
    this.log(message, 'warn', extra);
  }

  public error(message: string, extra?: Record<string, any>) {
    this.log(message, 'error', extra);
  }

  public debug(message: string, extra?: Record<string, any>) {
    this.log(message, 'debug', extra);
  }

  private bindConsole() {
    if (typeof console === 'undefined') return;

    const originalLog = console.log;
    const originalWarn = console.warn;
    const originalError = console.error;
    const originalInfo = console.info;

    console.log = (...args: any[]) => {
      originalLog.apply(console, args);
      this.log(args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' '), 'info', { consoleCaptured: true });
    };

    console.info = (...args: any[]) => {
      originalInfo.apply(console, args);
      this.log(args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' '), 'info', { consoleCaptured: true });
    };

    console.warn = (...args: any[]) => {
      originalWarn.apply(console, args);
      this.log(args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' '), 'warn', { consoleCaptured: true });
    };

    console.error = (...args: any[]) => {
      originalError.apply(console, args);
      this.log(args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' '), 'error', { consoleCaptured: true });
    };
  }

  private bindErrorListeners() {
    if (typeof window === 'undefined') return;

    window.addEventListener('error', (event) => {
      const errorMsg = event.error ? event.error.stack || event.error.message : event.message;
      this.log(errorMsg || 'Unhandled window error', 'fatal', {
        filename: event.filename,
        lineno: event.lineno,
        colno: event.colno,
      });
    });

    window.addEventListener('unhandledrejection', (event) => {
      const reason = event.reason;
      const errorMsg = reason instanceof Error ? reason.stack || reason.message : String(reason);
      this.log(`Unhandled Promise Rejection: ${errorMsg}`, 'fatal');
    });
  }

  public async flush(): Promise<void> {
    if (this.logQueue.length === 0) return;

    const batch = [...this.logQueue];
    this.logQueue = [];

    try {
      const response = await fetch(`${this.serverUrl}/logit/ingest`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${this.ingestKey}`
        },
        body: JSON.stringify({ logs: batch }),
      });

      if (!response.ok) {
        // Fallback: put logs back at front of queue if server rejected
        this.logQueue = [...batch, ...this.logQueue];
      }
    } catch (err) {
      // Network error, restore batch to queue
      this.logQueue = [...batch, ...this.logQueue];
    }
  }

  /**
   * Synchronous flush for beforeunload hooks.
   */
  private flushSync(): void {
    if (this.logQueue.length === 0) return;

    const batch = [...this.logQueue];
    this.logQueue = [];

    const payload = JSON.stringify({ logs: batch });

    if (typeof navigator !== 'undefined' && navigator.sendBeacon) {
      const url = `${this.serverUrl}/logit/ingest`;
      const urlWithToken = `${url}?token=${encodeURIComponent(this.ingestKey)}`;
      navigator.sendBeacon(urlWithToken, payload);
    } else {
      try {
        fetch(`${this.serverUrl}/logit/ingest`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${this.ingestKey}`
          },
          body: payload,
          keepalive: true
        });
      } catch (e) {
        // Ignore errors on close
      }
    }
  }

  public destroy() {
    if (this.timer) {
      clearInterval(this.timer);
    }
  }
}
