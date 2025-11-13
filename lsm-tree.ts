import BTree from "./b_tree/b_tree.ts";
import { SuperblockManager } from "./superblock";
import { WAL_Manager } from "./wal";
import type { EventRing } from "./event-ring";
import { OP_INV } from "./constants";
import { log, LogLevel } from "./utils";

export class LSM {
    memTable = new BTree<string, string>();
    freezeTable: BTree | null = null
    max_size: number;
    recoverFlush: bigint = BigInt(-1)

    constructor(max: number = 8) {
        this.max_size = max;
    }

    async recover(wal: WAL_Manager, sbm: SuperblockManager, er: EventRing) {
        const scan = wal.scan(wal.getHead(), wal.getUsed());
        const requests = await scan;
        log(LogLevel.info, "Starting recovery", { requests: requests.length, bytes: wal.getUsed() });
        const beforeRecov = wal.getLastLSN()
        this.recoverFlush = beforeRecov
        requests.forEach((v) => {
            er.dispatch({
                op: OP_INV[v.op]!,
                key: v.key,
                value: v.value,
                next: null,
                ts: 0,
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

    freeze() {
        this.freezeTable = this.memTable.clone()
        this.freezeTable.freeze()
    }
    needsFlush() {
        if (this.memTable.size >= this.max_size) {
            return true

        }
    }
}
