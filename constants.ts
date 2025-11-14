export const BLOCK = 4096;

export const SB_A_OFF = 0;

export const SB_B_OFF = BLOCK;

export const J_START = 2 * BLOCK;

export const J_LENGTH = 256 * BLOCK;

export const MANIFEST_OFF = J_START + J_LENGTH;

export const OP = { set: 1, del: 2, get: 3, check: 4 } as const;

export type Op = keyof typeof OP;

export const OP_INV: Record<number, Op> = { 1: "set", 2: "del", 3: "get", 4: "check" };

export const MAX_INFLIGHT = 8;

export const PREFIX = 16;

export const ENTRY_SIZE = 2 /*level*/ + 2 /*reserved*/ + 8 /*meta_off*/ + 4 /*meta_len*/ + PREFIX + PREFIX; // 48

export const HEADER_SIZE = 2 /*version*/ + 2 /*reserved*/ + 8 /*epoch*/ + 2 /*count*/ + 2 /*reserved*/; // 16

export const CAP = Math.floor((BLOCK - HEADER_SIZE) / ENTRY_SIZE);

// Error codes (unique across all modules)
export const WAL_RESULT = {
    OK: 0,
    WAL_FULL: 1001,
    LSN_NOT_FOUND: 1002,
} as const;

export const TABLE_RESULT = {
    OK: 0,
    INVALID_KEY_SIZE: 2001,
    TRUNCATED_ID: 2002,
    TRUNCATED_EXTENTS: 2003,
    MANIFEST_FULL: 2004,
    NEEDS_COMPACTION: 2005,
    BROKEN_TABLE_SIZE: 2006,
    ENTRY_NOT_EXIST: 2007,
    INVALID_PREFIX_SIZE: 2008,
} as const;

export const MANIFEST_RESULT = {
    OK: 0,
    INVALID_PREFIX_SIZE: 3001,
    TOO_MANY_ENTRIES: 3002,
    INVALID_PAGE_SIZE: 3003,
    COUNT_EXCEEDS_CAP: 3004,
    CORRUPT: 3005,
} as const;

export const FILE_RESULT = {
    OK: 0,
    SHORT_READ: 4001,
} as const;

export const SUPERBLOCK_RESULT = {
    OK: 0,
    NO_VALID_SUPERBLOCKS: 5001,
    NOT_INITIALIZED: 5002,
} as const;

export const CLOCK_RESULT = {
    OK: 0,
    CORRUPTED_EPOCH: 6001,
    BROKEN_CLOCK_STATE: 6002,
} as const;

export class DatabaseError extends Error {
    constructor(public code: number, message: string) {
        super(message);
        this.name = "DatabaseError";
    }
}


// build time constants
// These get injected by the build process (see build.ts)
// Default to development values when not built

declare const BUILD_TIME: string;
declare const VERSION: string;
declare const COMMIT: string;
declare const PRODUCTION: boolean;

const _BUILD_TIME = typeof BUILD_TIME !== "undefined" ? BUILD_TIME : "dev";
const _VERSION = typeof VERSION !== "undefined" ? VERSION : "dev";
const _COMMIT = typeof COMMIT !== "undefined" ? COMMIT : "dev";
const _PRODUCTION = typeof PRODUCTION !== "undefined" ? PRODUCTION : false;

export function version() {
    return { BUILD_TIME: _BUILD_TIME, VERSION: _VERSION, COMMIT: _COMMIT, PRODUCTION: _PRODUCTION }
}

export const LOG_LEVEL = _PRODUCTION ? 1 : 0;
