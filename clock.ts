import { CLOCK_RESULT, DatabaseError, SUPERBLOCK_RESULT } from "./constants";
import type { SuperblockManager } from "./superblock";
import { log, LogLevel } from "./utils";

export class Clock {
    private _epoch = 0n;
    private baseTime = BigInt(Date.now()) * 1_000_000n;
    private baseHrtime = process.hrtime.bigint();
    private current = () => this.baseTime + (process.hrtime.bigint() - this.baseHrtime)
    private lastCurrent = 0n;

    constructor(system?: () => bigint) {
        if (system) this.current = system
        this.epoch = this.now
    }

    get epoch() {
        return this._epoch
    }

    set epoch(t: bigint) {
        if (t > this.now) {
            log(LogLevel.err, `persisted=${t} now=${this.now}`)
            throw new DatabaseError(CLOCK_RESULT.CORRUPTED_EPOCH, "Persisted epoch is corrupted")
        }
        this._epoch = t
    }

    loadEpoc(sbm: SuperblockManager) {
        if (sbm.current() === null) {
            throw new DatabaseError(SUPERBLOCK_RESULT.NO_VALID_SUPERBLOCKS, "No presisted epoch, super block is corrupt")
        }
        this.epoch = sbm.current()!.epoch
    }

    get now() {
        let newest = this.current()
        let retries = 0;
        while (this.lastCurrent >= newest) {
            this.lastCurrent = newest
            const delay = Math.min(Math.pow(2, retries) * 1e6, 1e9);
            Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, delay / 1e6);
            newest = this.current()
            retries++;
            if (retries === 8) throw new DatabaseError(CLOCK_RESULT.BROKEN_CLOCK_STATE, "Broken clock state")
        }
        this.lastCurrent = newest;
        return newest;
    }

    get since() {
        return this.now - this.epoch
    }
}
