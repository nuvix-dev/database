/**
 * Minimal ANSI color helpers — zero-dependency replacement for chalk.
 * Colors are disabled automatically when stdout is not a TTY or when
 * NO_COLOR / FORCE_COLOR=0 is set.
 */

const enabled =
  process.env["NO_COLOR"] === undefined &&
  process.env["FORCE_COLOR"] !== "0" &&
  (process.stdout?.isTTY ?? false);

const wrap = (code: string, text: string): string =>
  enabled ? `\x1b[${code}m${text}\x1b[0m` : text;

export const colors = {
  red: (text: string) => wrap("31", text),
  green: (text: string) => wrap("32", text),
  yellow: (text: string) => wrap("33", text),
  blue: (text: string) => wrap("34", text),
  magenta: (text: string) => wrap("35", text),
  cyan: (text: string) => wrap("36", text),
  white: (text: string) => wrap("37", text),
  gray: (text: string) => wrap("90", text),
};
