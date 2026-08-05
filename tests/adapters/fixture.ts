import { readFileSync } from "node:fs";

export function readFixture(path: string): string {
  return readFileSync(new URL(`./fixtures/${path}`, import.meta.url), "utf8");
}

export function jsonFixture<T = unknown>(path: string): T {
  return JSON.parse(readFixture(path)) as T;
}
