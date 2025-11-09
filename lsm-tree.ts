import BTree from "sorted-btree";
import { appendFileSync } from "node:fs";
import { BLOCK, SuperblockManager } from "./superblock";
import { alignUp, type FileIO } from "./file-manager";
import { write } from "node:console";
import { WAL_Manager } from "./wal";
import { createHash } from "node:crypto";
import type { EventRing } from "./event-ring";
import { OP_INV } from "./types";

const PREFIX = 16; // set to 32 if you want 32-byte min/max prefixes

// Sizes
const ENTRY_SIZE = 2 /*level*/ + 2 /*reserved*/ + 8 /*meta_off*/ + 4 /*meta_len*/ + PREFIX + PREFIX; // 48 (with 16-byte prefixes)
const HEADER_SIZE = 2 /*version*/ + 2 /*reserved*/ + 8 /*epoch*/ + 2 /*count*/ + 2 /*reserved*/;    // 16
const CAP = Math.floor((BLOCK - HEADER_SIZE) / ENTRY_SIZE);
export const MANIFEST_OFF = WAL_Manager.J_START + WAL_Manager.J_LENGTH;

type Extent = { startBlock: number; blocks: number };

type TableMeta = {
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

// Every flush creates new manifest entry
// Once flush is completely written the entry becomes sealed and immutable. A single level (example L0) has multiple manifest entries from the flushes.
// So in theory a single manifest entry could have one extent if the number of pairs stored doesn't exceed a block. otherwise it would be a continguous 
// chain of blocks starting from startBlock offset in tableMeta.
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

function encodeManifestEntry(me: ManifestEntry): Uint8Array {
    if (me.minPrefix.length !== PREFIX || me.maxPrefix.length !== PREFIX) {
        throw new Error(`prefixes must be exactly ${PREFIX} bytes`);
    }
    const buf = new Uint8Array(ENTRY_SIZE);
    const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    let o = 0;
    v.setUint16(o, me.level, true); o += 2;
    v.setUint16(o, 0, true); o += 2; // reserved
    v.setBigUint64(o, me.metaOff, true); o += 8;
    v.setUint32(o, me.metaLen, true); o += 4;
    buf.set(me.minPrefix, o); o += PREFIX;
    buf.set(me.maxPrefix, o); o += PREFIX;
    return buf;
}

export function encodeManifestPage(mp: ManifestPage): Uint8Array {
    if (mp.entries.length > CAP) {
        throw new Error(`too many entries: ${mp.entries.length} > cap ${CAP}`);
    }
    const buf = new Uint8Array(BLOCK);
    const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    let o = 0;

    const version = mp.version ?? 1;

    // Header
    v.setUint16(o, version, true); o += 2;
    v.setUint16(o, 0, true); o += 2; // reserved
    v.setBigUint64(o, mp.epoch, true); o += 8;
    v.setUint16(o, mp.entries.length, true); o += 2;
    v.setUint16(o, 0, true); o += 2; // reserved

    // Entries
    for (const e of mp.entries) {
        const data = encodeManifestEntry(e);
        buf.set(data, o);
        o += ENTRY_SIZE; // advance!
    }

    // Remaining bytes are zero (already)
    return buf;
}
function encEntry(e: ManifestEntry): Uint8Array {
    const buf = new Uint8Array(ENTRY_SIZE);
    const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    let o = 0;
    v.setUint16(o, e.level, true); o += 2;
    v.setBigUint64(o, e.metaOff, true); o += 8;
    v.setUint32(o, e.metaLen, true); o += 4;
    buf.set(e.minPrefix.subarray(0, PREFIX), o); o += PREFIX;
    buf.set(e.maxPrefix.subarray(0, PREFIX), o);
    return buf;
}


export function decodeManifestPage(buf: Uint8Array): ManifestPage {
    if (buf.length !== BLOCK) throw new Error(`manifest page must be ${BLOCK} bytes`);

    const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    let o = 0;

    const version = v.getUint16(o, true); o += 2;
    o += 2; // reserved
    const epoch = v.getBigUint64(o, true); o += 8;
    const count = v.getUint16(o, true); o += 2;
    o += 2; // reserved

    // Empty/uninitialized page → return empty manifest
    const allZero =
        version === 0 && epoch === 0n && count === 0;
    if (allZero) return { epoch: 0n, version: 0, entries: [] };

    if (count > CAP) {
        throw new Error(`manifest count ${count} exceeds cap ${CAP}`);
    }
    const need = HEADER_SIZE + count * ENTRY_SIZE;
    if (need > buf.length) {
        throw new Error(`manifest corrupt: need ${need}, have ${buf.length}`);
    }

    const entries: ManifestEntry[] = [];
    for (let i = 0; i < count; i++) {
        const eOff = HEADER_SIZE + i * ENTRY_SIZE;
        const ev = new DataView(buf.buffer, buf.byteOffset + eOff, ENTRY_SIZE);
        let eo = 0;

        const level = ev.getUint16(eo, true); eo += 2;
        eo += 2; // reserved
        const metaOff = ev.getBigUint64(eo, true); eo += 8;
        const metaLen = ev.getUint32(eo, true); eo += 4;

        const minPrefix = buf.subarray(eOff + eo, eOff + eo + PREFIX); eo += PREFIX;
        const maxPrefix = buf.subarray(eOff + eo, eOff + eo + PREFIX); eo += PREFIX;

        entries.push({
            level,
            metaOff,
            metaLen,
            minPrefix: new Uint8Array(minPrefix),
            maxPrefix: new Uint8Array(maxPrefix),
        });
    }
    return { epoch, version, entries };
}


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
export class TableIO {
    private manifest: ManifestPage = { epoch: 0n, entries: [] };
    private tableTail: number = MANIFEST_OFF + BLOCK;

    constructor(private file: FileIO) { }

    initFrom(mp: ManifestPage) {
        this.manifest = mp;
    }

    async formatInitial({ version = 1, epoch = 1n }) {
        const mp: ManifestPage = {
            epoch,
            version,
            entries: [],
        };
        const buf = encodeManifestPage(mp);
        await this.file.write(MANIFEST_OFF, buf);
        await this.file.fsync();
        this.manifest = mp;
    }

    get manifestEntryTail() {
        return MANIFEST_OFF + HEADER_SIZE + this.manifest.entries.length * ENTRY_SIZE;
    }


    async load() {
        const mp_buf = await this.file.read(MANIFEST_OFF, BLOCK);
        // If file is empty here, you might get an all-zero page; handle as empty manifest.
        const mp = decodeManifestPage(mp_buf);
        this.manifest = mp;

        this.manifest.entries.forEach((e) => {
            this.tableTail += alignUp(e.metaLen, BLOCK)
        })
        return this;
    }
    private async updateManifest() {
        const buf = encodeManifestPage(this.manifest);
        await this.file.write(MANIFEST_OFF, buf);
        await this.file.fsync();
    }
    private async addEntry(e: ManifestEntry) {
        if (this.manifest.entries.length >= CAP) throw new Error("Ran out of manifest entries")
        this.manifest.entries.push(e)
        this.updateManifest()
        await this.file.write(this.manifestEntryTail, encodeManifestEntry(e))
        await this.file.fsync()
    }
    private async popEntry() {
        this.manifest.entries.pop()
        this.updateManifest()
    }

    private async requestTable(level: number, size: number, minPrefix: Uint8Array, maxPrefix: Uint8Array) {
        const left = await this.file.size() - this.tableTail
        if (this.tableTail + size > left) {
            throw new Error("Cannot add another table. Needs compaction");
        }
        const e = {
            level: level,
            metaOff: BigInt(this.tableTail),
            metaLen: this.tableTail + size,
            minPrefix,
            maxPrefix
        }
        await this.addEntry(e)
        return e
    }

    concat(parts: Uint8Array[]) {
        const total = parts.reduce((s, p) => s + p.length, 0);
        const out = new Uint8Array(total);
        let o = 0;
        for (const p of parts) { out.set(p, o); o += p.length; }
        return out;
    };

    // Flush WAL
    // Request a table of N size
    //

    async flushWAL(tree: BTree<string, string>) {
        let enc = new TextEncoder()
        // 1) Build data blocks (each <= BLOCK then padded to BLOCK)
        const blocks: Uint8Array[] = [];
        type IndexEntry = { firstKey: Uint8Array; off: number; len: number };
        const index: IndexEntry[] = [];

        let countInBlock = 0;
        let parts: Uint8Array[] = [];
        // block header: count u16
        let curSize = 2;
        let firstKeyThisBlock: Uint8Array | null = null;

        let minPrefix: Uint8Array | null = null;
        let maxPrefix: Uint8Array | null = null;
        let entryCount = 0;

        const pushRecord = (kb: Uint8Array, vb: Uint8Array) => {
            const recHdr = new Uint8Array(2 + 4);
            const dv = new DataView(recHdr.buffer);
            dv.setUint16(0, kb.length, true);
            dv.setUint32(2, vb.length, true);
            const recLen = recHdr.length + kb.length + vb.length;

            if (curSize + recLen > BLOCK) flushBlock();

            if (!firstKeyThisBlock) firstKeyThisBlock = kb;

            parts.push(recHdr, kb, vb);
            curSize += recLen;
            countInBlock++;
            entryCount++;

            const p = extractSortKey16(kb);
            if (!minPrefix || lt16(p, minPrefix)) minPrefix = p;
            if (!maxPrefix || gt16(p, maxPrefix)) maxPrefix = p;
        };

        const flushBlock = () => {
            if (countInBlock === 0) return;
            const hdr = new Uint8Array(2);
            new DataView(hdr.buffer).setUint16(0, countInBlock, true);
            const raw = this.concat([hdr, ...parts]);
            const padded = (() => {
                const need = alignUp(raw.length, BLOCK);
                if (need === raw.length) return raw;
                const out = new Uint8Array(need);
                out.set(raw, 0);
                return out;
            })();

            const off = blocks.reduce((s, b) => s + b.length, 0);
            blocks.push(padded);
            index.push({ firstKey: firstKeyThisBlock!, off, len: padded.length });

            // reset
            parts = [];
            curSize = 2;
            countInBlock = 0;
            firstKeyThisBlock = null;
        };

        tree.forEachPair((k, v) => {
            const kb = enc.encode(k);
            const vb = enc.encode(v);
            pushRecord(kb, vb);
        });
        flushBlock();

        if (blocks.length === 0) return; // nothing to flush

        // 2) Build index (firstKeyLen u16 + firstKey + off u64 + len u32)*
        const indexParts: Uint8Array[] = [];
        for (const e of index) {
            const hdr = new Uint8Array(2 + 8 + 4);
            const dv = new DataView(hdr.buffer);
            dv.setUint16(0, e.firstKey.length, true);
            dv.setBigUint64(2, BigInt(e.off), true);
            dv.setUint32(10, e.len, true);
            indexParts.push(hdr, e.firstKey);
        }
        const indexRaw = this.concat(indexParts);
        const indexBuf = (() => {
            const need = alignUp(indexRaw.length, 8);
            if (need === indexRaw.length) return indexRaw;
            const out = new Uint8Array(need);
            out.set(indexRaw, 0);
            return out;
        })();
        const indexOff = blocks.reduce((s, b) => s + b.length, 0);

        // 3) Footer (indexOff u64, indexLen u32, entryCount u32, blockSize u16)
        const footer = new Uint8Array(8 + 4 + 4 + 2);
        const fv = new DataView(footer.buffer);
        fv.setBigUint64(0, BigInt(indexOff), true);
        fv.setUint32(8, indexRaw.length, true);
        fv.setUint32(12, entryCount, true);
        fv.setUint16(16, BLOCK, true);

        const tableBytes = this.concat([...blocks, indexBuf, footer]);
        const sizeBytes = tableBytes.length;
        const blocksNeeded = Math.ceil(sizeBytes / BLOCK);

        console.log(`table bytes=${sizeBytes} blocks=${blocksNeeded}`);

        const extents = await this.requestTable(
            0,
            sizeBytes,
            minPrefix ?? new Uint8Array(16),
            maxPrefix ?? new Uint8Array(16),
        );
        console.log(this.manifest)
        // const tio = new ExtentIO(this.file, extents, BLOCK);
        // await tio.write(0, tableBytes);
        // await this.file.fsync();
        //
        // // 5) Build and store TableMeta, then WAL manifest update
        // const meta: TableMeta = {
        //     id: crypto.randomUUID(),
        //     level: 0,
        //     minKey: minPrefix ?? new Uint8Array(16),
        //     maxKey: maxPrefix ?? new Uint8Array(16),
        //     seqMin: 0n,
        //     seqMax: 0n,
        //     extents,
        //     sizeBytes,
        //     blockSize: BLOCK,
        //     indexOff,
        //     indexLen: indexRaw.length,
        //     entryCount,
        // };
    }
}

export class LSM {
    private memTable = new BTree<string, string>();
    max_size: number;

    constructor(max: number = 8, private tio: TableIO) {
        this.max_size = max;
    }

    async recover(wal: WAL_Manager, er: EventRing, sbm: SuperblockManager) {
        const scan = wal.scan(wal.getHead(), wal.getUsed());
        (await scan).forEach((v) => {
            er.dispatch({
                op: OP_INV[v.op]!,
                key: v.key,
                value: v.value,
                next: null,
                ts: 0,
                onComplete() {
                    if (wal.getUsed() > 0) {
                        wal.checkpoint(wal.getLastLSN(), sbm)
                    }
                }
            })
        })
    }

    async put(key: string, value: string) {
        this.memTable.set(key, value);
        if (this.memTable.length > this.max_size) {
            await this.tio.flushWAL(this.memTable)
        }
    }

    get(key: string) {
        const inMem = this.memTable.has(key);
        if (inMem) return this.memTable.get(key);
    }

    needsCompaction() {
        if (this.memTable.size >= 10) return true
    }
}
