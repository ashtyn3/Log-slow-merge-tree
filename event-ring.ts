import { appendFileSync } from "node:fs";
import { IntrusiveQueue, type Link } from "./intrusive-queue";
import { LSM } from "./lsm-tree";
import { TableIO } from "./table";
import { WAL_Manager } from "./wal.ts";
import { SuperblockManager } from "./superblock";
import type { Op } from "./constants";
import { MAX_INFLIGHT } from "./constants";
import { log, LogLevel } from "./utils";
import type { Clock } from "./clock.ts";

export type Operation = Link<Operation> & {
    op: Op,
    key: string,
    value?: string
    ts: bigint,
    onComplete?: (r: string) => void
}

export class EventRing {
    private q = new IntrusiveQueue<Operation>();
    private running: boolean = false


    constructor(
        private tree: LSM,
        private walManager: WAL_Manager,
        private tio: TableIO,
        private time: Clock,
        private sbManager?: SuperblockManager,
    ) {
    }

    start() {
        this.running = true
    }

    halt() {
        this.running = false
    }

    dispatch(op: Operation) {
        op.ts = this.time.now;
        log(LogLevel.debug, "Operation dispatched", { op: op.op, key: op.key });
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
        const ms_dur = BigInt(ms * 1000000);
        const end = this.time.now + ms_dur;

        while (true) {
            if (this.time.now >= end) break;

            const batch = this.q.takeUpTo(MAX_INFLIGHT);

            if (batch.length === 0) {
                // Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 0);
                continue;
            }

            if (this.tree.recoverFlush > 0n) {
                this.tree.recoverFlush = BigInt(-1)
            } else {
                await this.walManager.appendMany(batch)
            }

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
                this.tree.freeze();
                this.tree.memTable.clear()
                if (this.tree.freezeTable) {
                    await this.tio.flushWAL(this.tree.freezeTable)
                    await this.walManager.checkpoint(this.walManager.getLastLSN(), this.sbManager!)
                }
            }
        }
        // if (this.walManager.isDirty() && this.tree.needsCompaction()) {
        //     await this.walManager.checkpoint(this.walManager.getLastLSN(), this.sbManager!)
        // }
    }

    async tick() {
        if (!this.running) return

        await this.runFor(10);

        setImmediate(() => this.tick())
    }
}
