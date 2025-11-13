import { createHash } from "node:crypto";
import { LOG_LEVEL } from "./constants";

export function extractSortKey16(
    bytes: Uint8Array,
): Uint8Array {
    const h = createHash("blake2b512").update(bytes).digest(); // 64 bytes
    return new Uint8Array(h.buffer, h.byteOffset, 16); // first 16 bytes as prefix
}

export function cmp16(a: Uint8Array, b: Uint8Array): number {
    // Assumes a.length === 16 and b.length === 16
    for (let i = 0; i < 16; i++) {
        const d = a[i]! - b[i]!;
        if (d !== 0) return d; // < 0 => a < b, > 0 => a > b
    }
    return 0;
}

export function lt16(a: Uint8Array, b: Uint8Array): boolean {
    return cmp16(a, b) < 0;
}
export function gt16(a: Uint8Array, b: Uint8Array): boolean {
    return cmp16(a, b) > 0;
}

export function between16(
    x: Uint8Array,
    loIncl: Uint8Array,
    hiExcl: Uint8Array,
): boolean {
    return cmp16(x, loIncl) >= 0 && cmp16(x, hiExcl) < 0;
}

export enum LogLevel {
    debug = 0,
    info = 1,
    warn = 2,
    err = 3,
}

export function log(level: LogLevel, message: string, ...args: any[]) {
    if (level >= LOG_LEVEL) {
        const levelStr = LogLevel[level].toUpperCase();
        const timestamp = new Date().toISOString();
        console.error(`${timestamp} ${levelStr}: ${message}`, ...args);
    }
}
