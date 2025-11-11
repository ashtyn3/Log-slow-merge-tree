export type Op = "set" | "del" | "get" | "check";
export const OP = { set: 1, del: 2, get: 3, check: 4 } as const;
export const OP_INV: Record<number, Op> = { 1: "set", 2: "del", 3: "get", 4: "check" };

export const MAX_INFLIGHT = 8;

// LSM Tree types
export const PREFIX = 16; // set to 32 if you want 32-byte min/max prefixes

export type Extent = { startBlock: number; blocks: number };

export type IndexEntry = {
    firstKey: Uint8Array;
    off: number; // table-relative byte offset (u64 on disk)
    len: number; // block length (u32 on disk)
};

export type TableMeta = {
    id: string;
    level: number;
    minKey: Uint8Array;
    maxKey: Uint8Array;
    seqMin: bigint;
    seqMax: bigint;
    extents: Extent[];
    sizeBytes: number;
    blockSize: number;
    indexOff: number;
    indexLen: number;
    entryCount: number;
};

export type ManifestEntry = {
    level: number;          // u16
    metaOff: bigint;        // u64 (file offset)
    metaLen: number;        // u32
    minPrefix: Uint8Array;  // length must be PREFIX
    maxPrefix: Uint8Array;  // length must be PREFIX
};

export type ManifestPage = {
    epoch: bigint;          // u64
    version?: number;       // u16 (default 1)
    entries: ManifestEntry[];
};
