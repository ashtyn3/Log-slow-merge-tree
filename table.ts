import BTree from "./b_tree/b_tree.ts";
import { BLOCK } from "./constants";
import { alignUp, type FileIO } from "./file-manager";
import { MANIFEST_OFF } from "./constants";
import type { Extent, IndexEntry, TableMeta, ManifestEntry, ManifestPage } from "./types";
import { extractSortKey16, cmp16, lt16, gt16, log, LogLevel } from "./utils";
import { encodeManifestPage, decodeManifestPage } from "./manifest";
import { PREFIX, ENTRY_SIZE, TABLE_RESULT, DatabaseError } from "./constants";

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
        throw new DatabaseError(TABLE_RESULT.INVALID_KEY_SIZE, `minKey/maxKey must be ${PREFIX} bytes`);
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
        throw new DatabaseError(TABLE_RESULT.TRUNCATED_ID, "decodeTableMeta: truncated id");
    }
    const id = dec.decode(buf.subarray(o, o + idLen)); o += idLen;

    const extents: Extent[] = [];
    for (let i = 0; i < extCnt; i++) {
        if (o + 8 + 4 > buf.length) throw new DatabaseError(TABLE_RESULT.TRUNCATED_EXTENTS, "decodeTableMeta: truncated extents");
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

export class TableIO {
    private manifest: ManifestPage = { epoch: 0n, entries: [] };
    private tableTail: number = MANIFEST_OFF + BLOCK;
    private map = new Map<BigInt, { table: TableMeta, index: IndexEntry[] }>()

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
        return MANIFEST_OFF + 16 + this.manifest.entries.length * 48;
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
        if (this.manifest.entries.length >= Math.floor((BLOCK - 16) / 48)) throw new DatabaseError(TABLE_RESULT.MANIFEST_FULL, "Ran out of manifest entries")
        this.manifest.entries.push(e)
        this.updateManifest()
        await this.file.write(this.manifestEntryTail, encodeManifestEntry(e))
        await this.file.fsync()
    }

    private async requestTable(
        level: number,
        size: number,
        minPrefix: Uint8Array,
        maxPrefix: Uint8Array,
    ) {
        const fileBytes = await this.file.size();
        const left = fileBytes - this.tableTail;
        if (size > left) throw new DatabaseError(TABLE_RESULT.NEEDS_COMPACTION, "Cannot add another table. Needs compaction");

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
        log(LogLevel.info, "Starting table flush", { entryCount: tree.size });
        let enc = new TextEncoder()
        const blocks: Uint8Array[] = [];
        type IndexEntryLocal = { firstKey: Uint8Array; off: number; len: number };
        let index: IndexEntryLocal[] = [];

        let countInBlock = 0;
        let parts: Uint8Array[] = [];
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

        tree.toArray().sort(([k, v], [k2, v2]) => {
            const k_b = enc.encode(k)
            const k2_b = enc.encode(k2)
            const k1_sorter = extractSortKey16(k_b)
            const k2_sorter = extractSortKey16(k2_b)
            return cmp16(k1_sorter, k2_sorter)
        }).forEach(([k, v]) => {
            const kb = enc.encode(k)
            const vb = enc.encode(v)
            pushRecord(kb, vb)
        })

        flushBlock();

        if (blocks.length === 0) return; // nothing to flush


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
        const indexOff = metaOff + BLOCK;

        // TableMeta must use absolute indexOff and unpadded indexLen
        const meta: TableMeta = {
            id: crypto.randomUUID(),
            level: 0,
            minKey: minPrefix ?? new Uint8Array(16),
            maxKey: maxPrefix ?? new Uint8Array(16),
            seqMin: 0n,
            seqMax: 0n,
            extents: [],
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
            throw new DatabaseError(TABLE_RESULT.BROKEN_TABLE_SIZE, `broken table size: ${full.byteLength} !== ${entry.metaLen}`);
        }

        // Write at reserved location
        await this.file.write(metaOff, full);
        await this.file.fsync();
        log(LogLevel.info, "Table flushed", { id: meta.id, sizeBytes, entryCount });
    }


    async readEntryHead(i: number) {
        const e = this.manifest.entries[i];
        if (!e) throw new DatabaseError(TABLE_RESULT.ENTRY_NOT_EXIST, "entry doesn't exist");

        if (this.map.has(e.metaOff)) {
            log(LogLevel.debug, "Head cache hit", { saved: e.metaLen })
            return this.map.get(e.metaOff)!
        }

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

        const res = { index: indexAbs, table };
        this.map.set(e.metaOff, res);
        return res
    }
    async aggHeads(level: number = 0) {
        return (await Promise.all(this.manifest.entries.map(async (_, i) => {
            const head = await this.readEntryHead(i)
            if (head.table.level === level) return head
        }))).filter((v) => v !== undefined)
    }

    async levelSize(level: number) {
        let count = 0;
        for (const head of await this.aggHeads(level)) {
            count += head.table.entryCount
        }
        return count;
    }
}

export class TableReader {
    block: Uint8Array = new Uint8Array(0)
    prevKey: Uint8Array = new Uint8Array(0)
    count: number = 0
    localPos: number = 0;
    iter: number = 0;
    i: number = 0;
    constructor(private file: FileIO, private meta: { index: IndexEntry[], table: TableMeta }) { }

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

function encodeManifestEntry(me: ManifestEntry): Uint8Array {
    if (me.minPrefix.length !== PREFIX || me.maxPrefix.length !== PREFIX) {
        throw new DatabaseError(TABLE_RESULT.INVALID_PREFIX_SIZE, `prefixes must be exactly ${PREFIX} bytes`);
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
