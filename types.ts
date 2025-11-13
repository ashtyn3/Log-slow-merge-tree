import { OP, OP_INV, MAX_INFLIGHT, PREFIX } from "./constants";
import type { Op } from "./constants";

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
