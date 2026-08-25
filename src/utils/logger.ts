import fs from "node:fs";
import path from "node:path";
import { colors } from "./colors.js";

type LogLevel = "error" | "warn" | "info" | "debug";

const LEVEL_COLORS: Record<LogLevel, (text: string) => string> = {
  error: colors.red,
  warn: colors.yellow,
  info: colors.green,
  debug: colors.blue,
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

  private sink?: Bun.FileSink;
  private flushIntervalMs = 100;
  private flushTimer?: ReturnType<typeof setInterval>;
  private rotating = false;

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
      this.initWriteStream();
    }

    // Register default error serializer
    this.registerSerializer(Error, (err) => {
      return `${err.name}: ${err.message}\n${err.stack}`;
    });
  }

  private initWriteStream() {
    try {
      if (!fs.existsSync(path.dirname(this.logFilePath!))) {
        fs.mkdirSync(path.dirname(this.logFilePath!), { recursive: true });
      }
      // Native FileSink — buffered in Zig, flushed on highWaterMark or
      // explicitly via flush(). Replaces fs.WriteStream.
      // NOTE: `append` is valid at runtime but missing from @types/bun 1.x
      // typings (which only declare highWaterMark), hence the cast.
      const writerOptions = { append: true } as unknown as {
        highWaterMark?: number;
      };
      this.sink = Bun.file(this.logFilePath!).writer(writerOptions);
      this.flushTimer = setInterval(
        () => void this.flushSink(),
        this.flushIntervalMs,
      );
    } catch (err) {
      console.error("Logger: Failed to initialize write stream", err);
    }
  }

  private async rotateFileIfNeeded() {
    if (!this.sink || !this.logFilePath || this.rotating) return;
    try {
      const stats = fs.statSync(this.logFilePath);
      if (stats.size >= this.maxFileSize) {
        this.rotating = true;
        const staleSink = this.sink;
        this.sink = undefined;
        await staleSink.end();
        const rotatedPath =
          this.logFilePath +
          "." +
          new Date().toISOString().replace(/[:.]/g, "-");
        fs.renameSync(this.logFilePath, rotatedPath);
        this.initWriteStream();
        this.rotating = false;
      }
    } catch {
      this.rotating = false;
      // Ignore stat errors (e.g., file not found)
    }
  }

  /**
   * Periodic maintenance: rotate if the file grew past maxFileSize, then
   * flush the native sink. Runs on the flush timer instead of per log line,
   * so statSync no longer sits on the hot path.
   */
  private async flushSink() {
    if (!this.sink) return;
    await this.rotateFileIfNeeded();
    this.sink?.flush();
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
    const color = LEVEL_COLORS[level] || colors.white;
    const timeStr = this.timestamp
      ? colors.gray(new Date().toISOString()) + " "
      : "";
    const contextStr = this.context ? colors.magenta(`[${this.context}] `) : "";
    const levelStr = color(level.toUpperCase().padEnd(5));
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

    // File output (no color codes). FileSink buffers natively — no
    // application-level buffering needed.
    if (this.sink) {
      const plainText = output.replace(/\x1b\[[0-9;]*m/g, "");
      this.sink.write(plainText + "\n");
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
    if (this.sink) {
      const sink = this.sink;
      this.sink = undefined;
      try {
        sink.flush();
        await sink.end();
      } catch {
        // Ignore errors during shutdown
      }
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
