import { FileIO } from "./file-manager";

export const BLOCK = 4096;
export const SB_A_OFF = 0;
export const SB_B_OFF = BLOCK;

export type Superblock = {
    version: number;          // u16
    blockSize: number;        // u16
    epoch: bigint;            // u64 (monotonic)
    checkpointLSN: bigint;    // u64
    jHead: bigint;            // u64
    jTail: bigint;            // u64
};

function encodeSB(sb: Superblock): Uint8Array {
    const buf = new Uint8Array(BLOCK);
    const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    let o = 0;
    // no magic/CRC to keep it minimal; add later if you want integrity
    v.setUint16(o, sb.version, true); o += 2;
    v.setUint16(o, sb.blockSize, true); o += 2;
    v.setBigUint64(o, sb.epoch, true); o += 8;
    v.setBigUint64(o, sb.checkpointLSN, true); o += 8;
    v.setBigUint64(o, sb.jHead, true); o += 8;
    v.setBigUint64(o, sb.jTail, true); o += 8;
    return buf;
}

function decodeSB(buf: Uint8Array): Superblock | null {
    if (buf.length < BLOCK) return null;
    const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    let o = 0;
    const version = v.getUint16(o, true); o += 2;
    const blockSize = v.getUint16(o, true); o += 2;
    const epoch = v.getBigUint64(o, true); o += 8;
    const checkpointLSN = v.getBigUint64(o, true); o += 8;
    const jHead = v.getBigUint64(o, true); o += 8;
    const jTail = v.getBigUint64(o, true); o += 8;

    if (blockSize !== BLOCK || version === 0) return null;
    return { version, blockSize, epoch, checkpointLSN, jHead, jTail };
}

export class SuperblockManager {
    private active: "A" | "B" = "A";
    private sb: Superblock | null = null;

    constructor(private file: FileIO) { }

    // First-time format: write both A and B identical
    async formatInitial({
        journalStart,
        epoch = 1n,
    }: {
        journalStart: number;
        epoch?: bigint;
    }): Promise<void> {
        const base: Superblock = {
            version: 1,
            blockSize: BLOCK,
            epoch,
            checkpointLSN: 0n,
            jHead: BigInt(journalStart),
            jTail: BigInt(journalStart),
        };
        const buf = encodeSB(base);
        await this.file.write(SB_A_OFF, buf);
        await this.file.write(SB_B_OFF, buf);
        await this.file.fsync();
        this.active = "A";
        this.sb = base;
    }

    // Boot: read both and pick the newer epoch (if tie, pick B)
    async load(): Promise<Superblock> {
        const a = await this.file.read(SB_A_OFF, BLOCK);
        const b = await this.file.read(SB_B_OFF, BLOCK);
        const sa = decodeSB(a);
        const sb = decodeSB(b);

        if (!sa && !sb) throw new Error("no valid superblocks");
        let chosen: Superblock;
        if (sa && sb) {
            if (sb.epoch > sa.epoch) {
                chosen = sb;
                this.active = "B";
            } else if (sa.epoch > sb.epoch) {
                chosen = sa;
                this.active = "A";
            } else {
                // epochs equal: prefer B to handle partial updates symmetrically
                chosen = sb;
                this.active = "B";
            }
        } else {
            chosen = (sa ?? sb)!;
            this.active = sa ? "A" : "B";
        }
        this.sb = chosen;
        return chosen;
    }

    // Persist new checkpoint/journal pointers via A/B flip
    async checkpoint(update: {
        checkpointLSN: bigint;
        jHead: bigint;
        jTail: bigint;
    }): Promise<void> {
        if (!this.sb) throw new Error("call load() or formatInitial() first");
        const next: Superblock = {
            version: this.sb.version,
            blockSize: BLOCK,
            epoch: this.sb.epoch + 1n,
            checkpointLSN: update.checkpointLSN,
            jHead: update.jHead,
            jTail: update.jTail,
        };
        const buf = encodeSB(next);
        const targetOff = this.active === "A" ? SB_B_OFF : SB_A_OFF;
        await this.file.write(targetOff, buf);
        await this.file.fsync();
        // flip active
        this.active = this.active === "A" ? "B" : "A";
        this.sb = next;
    }

    current(): Superblock | null {
        return this.sb;
    }
}
