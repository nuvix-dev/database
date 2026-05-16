import fs from "node:fs";
import path from "node:path";

type LogLevel = "error" | "warn" | "info" | "debug";

// ANSI escape codes — replaces chalk dependency entirely
const ANSI = {
  reset: "\x1b[0m",
  red: "\x1b[31m",
  yellow: "\x1b[33m",
  green: "\x1b[32m",
  blue: "\x1b[34m",
  gray: "\x1b[90m",
  magenta: "\x1b[35m",
  white: "\x1b[37m",
} as const;

const LEVEL_COLORS: Record<LogLevel, string> = {
  error: ANSI.red,
  warn: ANSI.yellow,
  info: ANSI.green,
  debug: ANSI.blue,
};

export interface LoggerOptions {
  enabled?: boolean;
  level?: LogLevel;
  context?: string;
  timestamp?: boolean;
  logFilePath?: string;
  maxFileSize?: number; // bytes
}

type Serializer = (obj: any) => string;

export class Logger {
  private enabled: boolean = true;
  private level: LogLevel;
  private context?: string;
  private timestamp: boolean;
  private logFilePath?: string;
  private maxFileSize: number;

  private writer?: ReturnType<ReturnType<typeof Bun.file>["writer"]>;
  private logBuffer: string[] = [];
  private flushIntervalMs = 100;
  private flushTimer?: ReturnType<typeof setInterval>;

  private serializers = new Map<Function, Serializer>();

  private static staticInstance?: Logger;

  constructor(options?: LoggerOptions) {
    this.enabled = options?.enabled ?? true;
    this.level = options?.level ?? "info";
    this.context = options?.context;
    this.timestamp = options?.timestamp ?? true;
    this.logFilePath = options?.logFilePath;
    this.maxFileSize = options?.maxFileSize ?? 5 * 1024 * 1024; // default 5MB

    if (this.logFilePath) {
      this.initWriter();
    }

    // Register default error serializer
    this.registerSerializer(Error, (err) => {
      return `${err.name}: ${err.message}\n${err.stack}`;
    });
  }

  private initWriter() {
    try {
      const dir = path.dirname(this.logFilePath!);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
      // Use Bun.file().writer() for high-perf buffered writes
      this.writer = Bun.file(this.logFilePath!).writer();
      this.flushTimer = setInterval(
        () => this.flushBuffer(),
        this.flushIntervalMs,
      );
    } catch (err) {
      console.error("Logger: Failed to initialize writer", err);
    }
  }

  private rotateFileIfNeeded() {
    if (!this.writer || !this.logFilePath) return;
    try {
      const stats = fs.statSync(this.logFilePath);
      if (stats.size >= this.maxFileSize) {
        this.writer.end();
        const rotatedPath =
          this.logFilePath +
          "." +
          new Date().toISOString().replace(/[:.]/g, "-");
        fs.renameSync(this.logFilePath, rotatedPath);
        this.initWriter();
      }
    } catch {
      // Ignore stat errors (e.g., file not found)
    }
  }

  private flushBuffer() {
    if (!this.writer || this.logBuffer.length === 0) return;
    const data = this.logBuffer.join("\n") + "\n";
    this.writer.write(data);
    this.writer.flush();
    this.logBuffer.length = 0;
  }

  private shouldLog(level: LogLevel): boolean {
    if (!this.enabled) return false;
    const levels: LogLevel[] = ["error", "warn", "info", "debug"];
    return levels.indexOf(level) <= levels.indexOf(this.level);
  }

  private serializeArg(arg: any): string {
    if (arg === null || arg === undefined) return String(arg);
    for (const [type, serializer] of this.serializers) {
      if (arg instanceof type) {
        try {
          return serializer(arg);
        } catch {
          return "[Serializer error]";
        }
      }
    }
    if (typeof arg === "object") {
      try {
        return JSON.stringify(arg, null, 2);
      } catch {
        return "[Unserializable object]";
      }
    }
    return String(arg);
  }

  private formatMessage(level: LogLevel, message: string, ...args: any[]) {
    const color = LEVEL_COLORS[level] || ANSI.white;
    const timeStr = this.timestamp
      ? `${ANSI.gray}${new Date().toISOString()}${ANSI.reset} `
      : "";
    const contextStr = this.context
      ? `${ANSI.magenta}[${this.context}]${ANSI.reset} `
      : "";
    const levelStr = `${color}${level.toUpperCase().padEnd(5)}${ANSI.reset}`;
    const formattedArgs = args.length
      ? " " + args.map((a) => this.serializeArg(a)).join(" ")
      : "";
    return `${timeStr}${levelStr} ${contextStr}${message}${formattedArgs}`;
  }

  private log(level: LogLevel, message: string, ...args: any[]) {
    if (!this.shouldLog(level)) return;

    const output = this.formatMessage(level, message, ...args);

    // Console output
    if (level === "error" || level === "warn") {
      console.error(output);
    } else {
      console.log(output);
    }

    // File output (no color codes)
    if (this.writer) {
      const plainText = output.replace(/\x1b\[[0-9;]*m/g, "");
      this.logBuffer.push(plainText);
      if (this.logBuffer.length > 1000) {
        this.flushBuffer();
      }
      this.rotateFileIfNeeded();
    }
  }

  // Instance methods
  info(message: string, ...args: any[]) {
    this.log("info", message, ...args);
  }
  warn(message: string, ...args: any[]) {
    this.log("warn", message, ...args);
  }
  error(message: string, ...args: any[]) {
    this.log("error", message, ...args);
  }
  debug(message: string, ...args: any[]) {
    this.log("debug", message, ...args);
  }

  registerSerializer(type: Function, serializer: Serializer) {
    this.serializers.set(type, serializer);
  }

  async close() {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = undefined;
    }
    this.flushBuffer();
    if (this.writer) {
      this.writer.end();
    }
  }

  // Static singleton methods for quick usage
  private static getStaticInstance(): Logger {
    if (!this.staticInstance) {
      this.staticInstance = new Logger();
    }
    return this.staticInstance;
  }

  static info(message: string, ...args: any[]) {
    this.getStaticInstance().info(message, ...args);
  }
  static warn(message: string, ...args: any[]) {
    this.getStaticInstance().warn(message, ...args);
  }
  static error(message: string, ...args: any[]) {
    this.getStaticInstance().error(message, ...args);
  }
  static debug(message: string, ...args: any[]) {
    this.getStaticInstance().debug(message, ...args);
  }

  static registerSerializer(type: Function, serializer: Serializer) {
    this.getStaticInstance().registerSerializer(type, serializer);
  }

  static async close() {
    await this.getStaticInstance().close();
  }
}
