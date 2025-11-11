import { appendFileSync, readFileSync } from "node:fs";
import type { Op } from "./types";
import { OP, OP_INV } from "./types";
import { BLOCK, type FileIO } from "./file-manager";
import type { Operation } from "./event-ring";
import type { SuperblockManager } from "./superblock";

export class WAL_Manager {
    // Layout constants
    static readonly BLOCK = 4096;
    static readonly SB_A_OFF = 0;
    static readonly SB_B_OFF = WAL_Manager.BLOCK;
    static readonly J_START = 2 * WAL_Manager.BLOCK;
    static readonly J_LENGTH = 256 * 4096;
    lsnToEnd = new Map<bigint, number>();

    // Align helpers
    private alignUp(n: number, a = 8): number {
        return (n + (a - 1)) & ~(a - 1);
    }

    // State
    private lsn: bigint = 0n;
    private head: number; // byte offset inside journal region
    private tail: number; // byte offset inside journal region
    private readonly jStart: number;
    private readonly jEnd: number;

    // Record header sizes (no magic/CRC)
    // [lsn u64][op u8][klen u32][vlen u32]
    private readonly minHdr = 8 + 1 + 4 + 4;

    // Op code 0 = PAD/wrap
    private readonly OP_PAD = 0;

    constructor(
        private file: FileIO,
        opts: { journalBytes: number; jStart?: number } = { journalBytes: 256 * 4096 },
    ) {
        this.jStart = opts.jStart ?? WAL_Manager.J_START;
        this.jEnd = this.jStart + opts.journalBytes;
        this.head = this.jStart;
        this.tail = this.jStart;
    }

    // Formatting (first run)
    async format(totalSizeBytes: number) {
        await this.file.ensureSize(totalSizeBytes);
        // jHead = jTail = J_START for a fresh file
        this.head = this.jStart;
        this.tail = this.jStart;
        this.lsn = 0n;
        // If you keep superblocks, write them here (omitted for brevity)
    }

    // Boot from known jHead/jTail/LSN (e.g., after reading superblock+scan)
    initFrom(head: number, tail: number, lastLSN: bigint) {
        this.head = head;
        this.tail = tail;
        this.lsn = lastLSN;
    }

    // Encode one record (aligned)
    private encodeRecord(
        lsn: bigint,
        op: number,
        key: string,
        value?: string,
    ): Uint8Array {
        const enc = new TextEncoder();
        const kb = enc.encode(key);
        const vb = value !== undefined ? enc.encode(value) : new Uint8Array(0);

        const raw = new Uint8Array(this.minHdr + kb.length + vb.length);
        const v = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);

        let o = 0;
        v.setBigUint64(o, lsn, true);
        o += 8;
        v.setUint8(o, op);
        o += 1;
        v.setUint32(o, kb.length, true);
        o += 4;
        v.setUint32(o, vb.length, true);
        o += 4;

        raw.set(kb, o);
        o += kb.length;
        raw.set(vb, o);

        const out = new Uint8Array(this.alignUp(raw.length));
        out.set(raw, 0);
        return out;
    }

    // PAD record to mark wrap
    private encodePad(lsn: bigint): Uint8Array {
        const raw = new Uint8Array(this.minHdr);
        const v = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
        let o = 0;
        v.setBigUint64(o, lsn, true);
        o += 8;
        v.setUint8(o, this.OP_PAD);
        o += 1;
        v.setUint32(o, 0, true);
        o += 4;
        v.setUint32(o, 0, true);
        o += 4;
        const out = new Uint8Array(this.alignUp(raw.length));
        out.set(raw, 0);
        return out;
    }

    getUsed() {
        return this.tail >= this.head
            ? this.tail - this.head
            : (this.jEnd - this.head) + (this.tail - this.jStart);
    }

    // Append a batch; handles wrap; updates tail and LSNs
    async appendMany(items: Operation[]): Promise<bigint> {
        // Map op to a number if needed
        const toNumOp = (op: Op) => OP[op];

        // Pre-encode records with contiguous LSNs
        let next = this.lsn;
        const recs = items.map((it) => {
            next += 1n;
            return {
                lsn: next,
                buf: this.encodeRecord(next, toNumOp(it.op), it.key, it.value),
            };
        });

        const batchBytes = recs.reduce((s, r) => s + r.buf.length, 0);
        const padBytes = this.alignUp(this.minHdr);
        const regionBytes = this.jEnd - this.jStart;

        // Free-space in the ring
        const used = this.getUsed()
        const free = regionBytes - used;

        // Will this batch cross the end?
        const needsWrap = this.tail + batchBytes > this.jEnd;
        const needTotal = batchBytes + (needsWrap ? padBytes : 0);

        if (free < needTotal) {
            throw new Error(
                `WAL full: free=${free} (${(free / BLOCK) | 0} blocks) ` +
                `need=${needTotal} (${(needTotal / BLOCK) | 0} blocks) ` +
                `(batch=${batchBytes} wrap=${needsWrap})`,
            );
        }

        // If we cross the end, write PAD and wrap to jStart
        if (needsWrap) {
            const pad = this.encodePad(this.lsn); // PAD doesn't consume an LSN
            await this.file.write(this.tail, pad);
            this.tail = this.jStart;
        }

        // Write records and remember end offsets for checkpointing
        let off = this.tail;
        for (const r of recs) {
            await this.file.write(off, r.buf);
            off += r.buf.length;
            // normalize: if we ended exactly at jEnd, the next offset is jStart
            const normEnd = off === this.jEnd ? this.jStart : off;
            this.lsnToEnd.set(r.lsn, normEnd);
        }

        // Update pointers and last LSN
        this.tail = off === this.jEnd ? this.jStart : off;
        this.lsn = next;

        await this.file.fsync(); // per durability policy
        return next; // last LSN in this batch (use for checkpoint)
    }

    async checkpoint(lsn: bigint, sbm: SuperblockManager) {
        const offset = this.lsnToEnd.get(lsn)
        if (offset === undefined) {
            throw new Error(`cannot checkpoint: LSN ${lsn} not found`);
        }
        this.head = offset; // may equal jStart if we ended exactly at J_END
        for (const k of this.lsnToEnd.keys()) {
            if (k <= offset) this.lsnToEnd.delete(k);
        }
        await sbm.checkpoint({
            checkpointLSN: lsn,
            jHead: BigInt(this.head),
            jTail: BigInt(this.tail),
        });
    }

    // Decode one record at offset; returns next offset and decoded entry
    // If PAD encountered, returns kind: "pad" and next = aligned header end.
    decodeAt(view: DataView, offset: number):
        | { kind: "pad"; next: number }
        | { kind: "entry"; next: number; lsn: bigint; op: number; key: string; value?: string }
        | null {
        const end = view.byteLength;
        if (offset + this.minHdr > end) return null;

        const dec = new TextDecoder();
        let o = offset;
        const lsn = view.getBigUint64(o, true);
        o += 8;
        const op = view.getUint8(o);
        o += 1;
        const klen = view.getUint32(o, true);
        o += 4;
        const vlen = view.getUint32(o, true);
        o += 4;

        if (op === this.OP_PAD) {
            const next = this.alignUp(o); // header only
            return { kind: "pad", next };
        }

        const need = o + klen + vlen;
        if (need > end) return null;

        const keyBytes = new Uint8Array(view.buffer, view.byteOffset + o, klen);
        o += klen;
        const valBytes = new Uint8Array(view.buffer, view.byteOffset + o, vlen);
        o += vlen;

        const next = this.alignUp(o);
        return {
            kind: "entry",
            next,
            lsn,
            op,
            key: dec.decode(keyBytes),
            value: vlen > 0 ? dec.decode(valBytes) : undefined,
        };
    }

    // Simple scanner from a given offset (e.g., head) until a limit or invalid
    async scan(from: number, maxBytes: number): Promise<
        Array<{ lsn: bigint; op: number; key: string; value?: string }>
    > {
        const buf = await this.file.read(from, maxBytes);
        const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
        const out: Array<{ lsn: bigint; op: number; key: string; value?: string }> =
            [];

        let off = 0;
        while (off < view.byteLength) {
            const r = this.decodeAt(view, off);
            if (!r) break;
            if (r.kind === "pad") {
                // caller should jump to jStart when scanning the real file
                off = r.next;
                continue;
            }
            out.push({ lsn: r.lsn, op: r.op, key: r.key, value: r.value });
            off = r.next;
        }
        return out;
    }

    // Getters for persisting to superblock
    getHead(): number {
        return this.head;
    }
    getTail(): number {
        return this.tail;
    }
    getLastLSN(): bigint {
        return this.lsn;
    }

    isDirty() {
        if (this.getUsed() > 0) return true
        return false
    }

    // Advance head after checkpoint to cut the log (no truncation)
    advanceHeadTo(offset: number) {
        // must be within [jStart, jEnd) and aligned
        this.head = offset;
    }
}
