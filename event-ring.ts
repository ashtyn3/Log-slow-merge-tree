import { appendFileSync } from "node:fs";
import { IntrusiveQueue, type Link } from "./intrusive-queue";
import { LSM } from "./lsm-tree";
import { TableIO } from "./table";
import { WAL_Manager } from "./wal.ts";
import { SuperblockManager } from "./superblock";
import { type Op, MAX_INFLIGHT } from "./types";

export type Operation = Link<Operation> & {
    op: Op,
    key: string,
    value?: string
    ts: number,
    onComplete?: (r: string) => void
}

export class EventRing {
    private q = new IntrusiveQueue<Operation>();


    constructor(
        private tree: LSM,
        private walManager: WAL_Manager,
        private tio: TableIO,
        private sbManager?: SuperblockManager,
    ) {
    }

    dispatch(op: Operation) {
        op.ts = Bun.nanoseconds();
        this.q.push(op);
    }

    async submit(tree: LSM, op: Operation) {
        return new Promise<string>(async (r) => {
            switch (op.op) {
                case "check": {
                    setImmediate(() => this.walManager.checkpoint(this.walManager.getLastLSN(), this.sbManager!))
                    return r("");
                }
                case "set": {
                    await tree.put(this, op.key, op.value ?? "");
                    return r("");
                }
                case "get": {
                    const result = tree.get(op.key);
                    return r(result || "");
                }
            }
        });
    }


    async runFor(ms: number) {
        const ms_dur = (ms * 1000000);
        const end = Bun.nanoseconds() + ms_dur;


        while (true) {
            if (Bun.nanoseconds() >= end) break;

            const batch = this.q.takeUpTo(MAX_INFLIGHT);

            if (batch.length === 0) {
                // Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 0);
                continue;
            }

            await this.walManager.appendMany(batch)

            if (this.sbManager) {
                await this.sbManager.checkpoint({
                    checkpointLSN: this.walManager.getLastLSN(),
                    jHead: BigInt(this.walManager.getHead()),
                    jTail: BigInt(this.walManager.getTail()),
                });
            }

            for (const op of batch) {
                await new Promise<void>((r) => setImmediate(async () => {
                    const v = await this.submit(this.tree, op);
                    op.onComplete?.(v);
                    r();
                }));
            }
            if (this.tree.needsFlush()) {
                await this.tio.flushWAL(this.tree.memTable)
                this.tree.memTable.clear()
                await this.walManager.checkpoint(this.walManager.getLastLSN(), this.sbManager!)
            }
        }
        // if (this.walManager.isDirty() && this.tree.needsCompaction()) {
        //     await this.walManager.checkpoint(this.walManager.getLastLSN(), this.sbManager!)
        // }
    }
}
