import { open as openFile, type FileHandle } from "node:fs/promises";
import { BLOCK, FILE_RESULT, DatabaseError } from "./constants";

export const alignUp = (n: number, blk = BLOCK) =>
    (n + (blk - 1)) & ~(blk - 1);

export class FileIO {
    private fh!: FileHandle;
    constructor(private path: string) { }

    // Pass flag so we don't use O_APPEND
    async open(flag: "r+" | "w+" = "r+"): Promise<void> {
        this.fh = await openFile(this.path, flag);
    }

    async close(): Promise<void> {
        if (this.fh) await this.fh.close();
    }

    async size(): Promise<number> {
        const s = await this.fh.stat();
        return s.size;
    }

    // Preallocate using truncate (extends with zeros)
    async ensureSize(bytes: number): Promise<void> {
        const cur = await this.size();
        if (cur >= bytes) return;
        await this.fh.truncate(bytes);
        await this.fh.sync();
    }

    async write(offset: number, buf: Uint8Array): Promise<void> {
        // Works only if file NOT opened with O_APPEND
        await this.fh.write(buf, 0, buf.length, offset);
    }

    async writev(offset: number, bufs: readonly Uint8Array[]): Promise<void> {
        let off = offset;
        for (const b of bufs) {
            await this.fh.write(b, 0, b.length, off);
            off += b.length;
        }
    }

    async readExact(offset: number, len: number): Promise<Uint8Array> {
        const out = new Uint8Array(len);
        let filled = 0;
        while (filled < len) {
            const { bytesRead } = await this.fh.read(out, filled, len - filled, offset + filled);
            if (bytesRead === 0) throw new DatabaseError(FILE_RESULT.SHORT_READ, "short read");
            filled += bytesRead;
        }
        return out;
    }

    async read(offset: number, len: number): Promise<Uint8Array> {
        const out = new Uint8Array(len);
        const { bytesRead } = await this.fh.read(out, 0, len, offset);
        return out.subarray(0, bytesRead);
    }

    async fsync(): Promise<void> {
        await this.fh.sync();
    }
}
