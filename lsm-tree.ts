import BTree from "sorted-btree";
import { appendFileSync } from "node:fs";
import { BLOCK, SuperblockManager } from "./superblock";
import { alignUp, type FileIO } from "./file-manager";
import { write } from "node:console";
import { WAL_Manager } from "./wal";
import { createHash } from "node:crypto";
import type { EventRing } from "./event-ring";
import { OP_INV } from "./types";
import type { IndexKind } from "typescript";

const PREFIX = 16; // set to 32 if you want 32-byte min/max prefixes

// Sizes
const ENTRY_SIZE = 2 /*level*/ + 2 /*reserved*/ + 8 /*meta_off*/ + 4 /*meta_len*/ + PREFIX + PREFIX; // 48 (with 16-byte prefixes)
const HEADER_SIZE = 2 /*version*/ + 2 /*reserved*/ + 8 /*epoch*/ + 2 /*count*/ + 2 /*reserved*/;    // 16
const CAP = Math.floor((BLOCK - HEADER_SIZE) / ENTRY_SIZE);
export const MANIFEST_OFF = WAL_Manager.J_START + WAL_Manager.J_LENGTH;

type Extent = { startBlock: number; blocks: number };

type IndexEntry = {
    firstKey: Uint8Array;
    off: number; // table-relative byte offset (u64 on disk)
    len: number; // block length (u32 on disk)
};

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


export function decodeIndex(buf: Uint8Array): IndexEntry[] {
    const out: IndexEntry[] = [];
    const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    let o = 0;
    while (o + 2 + 8 + 4 <= buf.length) {
        const klen = dv.getUint16(o, true); o += 2;
        const off = Number(dv.getBigUint64(o, true)); o += 8;
        const len = dv.getUint32(o, true); o += 4;

        if (o + klen > buf.length) break; // tolerate padded tail or truncated view
        const key = buf.subarray(o, o + klen);
        o += klen;

        out.push({ firstKey: new Uint8Array(key), off, len });
    }
    return out;
}

export function encodeTableMeta(meta: TableMeta): Uint8Array {
    const enc = new TextEncoder();
    if (meta.minKey.length !== PREFIX || meta.maxKey.length !== PREFIX) {
        throw new Error(`minKey/maxKey must be ${PREFIX} bytes`);
    }
    const idBytes = enc.encode(meta.id);
    const extCnt = meta.extents.length;

    const headerLen =
        2 + // id_len
        2 + // level
        8 + // seqMin
        8 + // seqMax
        8 + // sizeBytes
        4 + // blockSize
        8 + // indexOff
        4 + // indexLen
        4 + // entryCount
        PREFIX + // minKey
        PREFIX + // maxKey
        4; // extents_count

    const extentsLen = extCnt * (8 + 4); // startBlock u64 + blocks u32
    const total = headerLen + idBytes.length + extentsLen;

    const buf = new Uint8Array(total);
    const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    let o = 0;

    v.setUint16(o, idBytes.length, true); o += 2;
    v.setUint16(o, meta.level, true); o += 2;

    v.setBigUint64(o, meta.seqMin, true); o += 8;
    v.setBigUint64(o, meta.seqMax, true); o += 8;

    v.setBigUint64(o, BigInt(meta.sizeBytes), true); o += 8;
    v.setUint32(o, meta.blockSize >>> 0, true); o += 4;

    v.setBigUint64(o, BigInt(meta.indexOff), true); o += 8;
    v.setUint32(o, meta.indexLen >>> 0, true); o += 4;

    v.setUint32(o, meta.entryCount >>> 0, true); o += 4;

    buf.set(meta.minKey, o); o += PREFIX;
    buf.set(meta.maxKey, o); o += PREFIX;

    v.setUint32(o, extCnt >>> 0, true); o += 4;

    // id bytes
    buf.set(idBytes, o); o += idBytes.length;

    // extents
    for (const e of meta.extents) {
        v.setBigUint64(o, BigInt(e.startBlock), true); o += 8;
        v.setUint32(o, e.blocks >>> 0, true); o += 4;
    }

    return buf;
}
export function decodeTableMeta(buf: Uint8Array): TableMeta {
    const dec = new TextDecoder();
    const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    let o = 0;

    const idLen = v.getUint16(o, true); o += 2;
    const level = v.getUint16(o, true); o += 2;

    const seqMin = v.getBigUint64(o, true); o += 8;
    const seqMax = v.getBigUint64(o, true); o += 8;

    const sizeBytes = Number(v.getBigUint64(o, true)); o += 8;
    const blockSize = v.getUint32(o, true); o += 4;

    const indexOff = Number(v.getBigUint64(o, true)); o += 8;
    const indexLen = v.getUint32(o, true); o += 4;

    const entryCount = v.getUint32(o, true); o += 4;

    const minKey = buf.subarray(o, o + PREFIX); o += PREFIX;
    const maxKey = buf.subarray(o, o + PREFIX); o += PREFIX;

    const extCnt = v.getUint32(o, true); o += 4;

    if (o + idLen > buf.length) {
        throw new Error("decodeTableMeta: truncated id");
    }
    const id = dec.decode(buf.subarray(o, o + idLen)); o += idLen;

    const extents: Extent[] = [];
    for (let i = 0; i < extCnt; i++) {
        if (o + 8 + 4 > buf.length) throw new Error("decodeTableMeta: truncated extents");
        const startBlock = Number(v.getBigUint64(o, true)); o += 8;
        const blocks = v.getUint32(o, true); o += 4;
        extents.push({ startBlock, blocks });
    }

    return {
        id,
        level,
        minKey: new Uint8Array(minKey),
        maxKey: new Uint8Array(maxKey),
        seqMin,
        seqMax,
        extents,
        sizeBytes,
        blockSize,
        indexOff,
        indexLen,
        entryCount,
    };
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

export function between16(
    x: Uint8Array,
    loIncl: Uint8Array,
    hiExcl: Uint8Array,
): boolean {
    return cmp16(x, loIncl) >= 0 && cmp16(x, hiExcl) < 0;
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


    private async requestTable(
        level: number,
        size: number,
        minPrefix: Uint8Array,
        maxPrefix: Uint8Array,
    ) {
        const fileBytes = await this.file.size();
        const left = fileBytes - this.tableTail;
        if (size > left) throw new Error("Cannot add another table. Needs compaction");

        const metaOff = this.tableTail;
        const metaLen = size; // length in bytes of the whole table blob you will write

        const e: ManifestEntry = {
            level,
            metaOff: BigInt(metaOff),
            metaLen,
            minPrefix,
            maxPrefix,
        };
        await this.addEntry(e);

        // Reserve bytes by bumping the tail now (so next placement won’t overlap)
        this.tableTail = alignUp(metaOff + metaLen, BLOCK);
        return e;
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
        let index: IndexEntry[] = [];

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
        let extents: Array<Extent> = [];

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
        // 

        const indexParts: Uint8Array[] = [];
        for (const e of index) {
            const hdr = new Uint8Array(2 + 8 + 4);
            const dv = new DataView(hdr.buffer);
            dv.setUint16(0, e.firstKey.length, true);
            dv.setBigUint64(2, BigInt(e.off), true); // off is relative to dataStart
            dv.setUint32(10, e.len, true);
            indexParts.push(hdr, e.firstKey);
        }
        const indexRaw = this.concat(indexParts);
        const indexBuf = (() => {
            const need = alignUp(indexRaw.length, 8);
            const out = new Uint8Array(need);
            out.set(indexRaw, 0);
            return out;
        })();
        const indexLen = indexRaw.length;
        const indexLenPadded = indexBuf.length;

        const blockBytes = blocks.reduce((s, b) => s + b.length, 0);
        const sizeBytes = BLOCK + indexLenPadded + blockBytes; // total table blob size

        // Reserve space in file for the full blob
        const entry = await this.requestTable(
            0,
            sizeBytes,
            minPrefix ?? new Uint8Array(16),
            maxPrefix ?? new Uint8Array(16),
        );
        const metaOff = Number(entry.metaOff);
        const indexOff = metaOff + BLOCK;                 // absolute file offset

        // TableMeta must use absolute indexOff and unpadded indexLen
        const meta: TableMeta = {
            id: crypto.randomUUID(),
            level: 0,
            minKey: minPrefix ?? new Uint8Array(16),
            maxKey: maxPrefix ?? new Uint8Array(16),
            seqMin: 0n,
            seqMax: 0n,
            extents: [],                // optional for this contiguous layout
            sizeBytes,
            blockSize: BLOCK,
            indexOff,                   // absolute
            indexLen,                   // unpadded
            entryCount,
        };

        // Encode meta into one block
        const encoded = encodeTableMeta(meta);
        const alignedMetaBlock = new Uint8Array(BLOCK);
        alignedMetaBlock.set(encoded, 0);

        // Compose full table blob: [meta block][index][blocks]
        const full = this.concat([alignedMetaBlock, indexBuf, ...blocks]);
        if (full.byteLength !== entry.metaLen) {
            throw new Error(`broken table size: ${full.byteLength} !== ${entry.metaLen}`);
        }

        // Write at reserved location
        await this.file.write(metaOff, full);
        await this.file.fsync();
    }


    async readEntryHead(i: number) {
        const e = this.manifest.entries[i];
        if (!e) throw new Error("entry doesn't exist");

        const metaOff = Number(e.metaOff);
        const metaBuf = await this.file.read(metaOff, BLOCK);
        const table = decodeTableMeta(metaBuf);

        const indexRaw = await this.file.read(table.indexOff, table.indexLen);
        const idxRel = decodeIndex(indexRaw);

        const dataStart = table.indexOff + alignUp(table.indexLen, 8);

        const indexAbs = idxRel.map(ent => ({
            firstKey: ent.firstKey,
            off: dataStart + ent.off, // ABSOLUTE FILE OFFSET
            len: ent.len,
        }));

        return { index: indexAbs, table };
    }
    async aggHeads() {
        return await Promise.all(this.manifest.entries.map(async (_, i) => {
            return this.readEntryHead(i)
        }))
    }
}

export class TableReader {
    block: Uint8Array = new Uint8Array(0)
    prevKey: Uint8Array = new Uint8Array(0)
    count: number = 0
    localPos: number = 0;
    iter: number = 0;
    i: number = 0;
    constructor(private file: FileIO, private tio: TableIO, private meta: { index: IndexEntry[], table: TableMeta }) { }

    async loadBlock(e: IndexEntry) {
        this.block = await this.file.read(e.off, e.len);
        const dv = new DataView(this.block.buffer, this.block.byteOffset);
        this.count = dv.getUint16(0, true);
        this.localPos = 2;
        this.iter = 0;
        this.prevKey = new Uint8Array(0);
    }


    async next() {
        while (true) {
            if (this.block.length === 0) {
                if (this.i >= this.meta.index.length) return null;
                await this.loadBlock(this.meta.index[this.i++]!);
            }
            if (this.iter >= this.count) {
                this.block = new Uint8Array(0);
                continue;
            }

            const dv = new DataView(this.block.buffer, this.block.byteOffset + this.localPos);
            const klen = dv.getUint16(0, true);
            const vlen = dv.getUint32(2, true);
            this.localPos += 2 + 4;

            const key = this.block.subarray(this.localPos, this.localPos + klen);
            this.localPos += klen;
            const value = this.block.subarray(this.localPos, this.localPos + vlen);
            this.localPos += vlen;

            this.iter++;
            return { key, value };
        }
    }
}

export class LSM {
    memTable = new BTree<string, string>();
    max_size: number;

    constructor(max: number = 8) {
        this.max_size = max;
    }

    async recover(wal: WAL_Manager, sbm: SuperblockManager, er: EventRing) {
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

    async put(er: EventRing, key: string, value: string) {
        this.memTable.set(key, value);
    }

    get(key: string) {
        const inMem = this.memTable.has(key);
        if (inMem) return this.memTable.get(key);
    }

    needsFlush() {
        if (this.memTable.size >= this.max_size) return true
    }
}
