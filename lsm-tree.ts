import BTree from "sorted-btree";
import { SuperblockManager } from "./superblock";
import { WAL_Manager } from "./wal";
import type { EventRing } from "./event-ring";
import { OP_INV } from "./types";
import { log, LogLevel } from "./utils";

export class LSM {
    memTable = new BTree<string, string>();
    max_size: number;
    recoverFlush = false

    constructor(max: number = 8) {
        this.max_size = max;
    }

    async recover(wal: WAL_Manager, sbm: SuperblockManager, er: EventRing) {
        const scan = wal.scan(wal.getHead(), wal.getUsed());
        const requests = await scan;
        log(LogLevel.info, "Starting recovery", { requests: requests.length, bytes: wal.getUsed() });
        requests.forEach((v) => {
            er.dispatch({
                op: OP_INV[v.op]!,
                key: v.key,
                value: v.value,
                next: null,
                ts: 0,
                onComplete() {
                    // if (wal.getUsed() > 0) {
                    //     wal.checkpoint(wal.getLastLSN(), sbm)
                    // }
                }
            })
        })
        // this.recoverFlush = true
    }

    async put(er: EventRing, key: string, value: string) {
        this.memTable.set(key, value);
    }

    get(key: string) {
        const inMem = this.memTable.has(key);
        if (inMem) return this.memTable.get(key);
    }

    needsFlush() {
        if (this.memTable.size >= this.max_size || this.recoverFlush) return true
    }
}
