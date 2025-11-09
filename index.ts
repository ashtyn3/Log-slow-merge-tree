import { FileIO } from "./file-manager";
import { SuperblockManager } from "./superblock";
import { WAL_Manager } from "./wal";
import { LSM } from "./lsm-tree";
import { EventRing } from "./event-ring";
import { TableIO } from "./lsm-tree";


const path = "wal.bin";
const io = new FileIO(path);
try {
    await io.open("r+");
} catch {
    await io.open("w+");
}

const wal = new WAL_Manager(io);
const sbm = new SuperblockManager(io);
let tio = new TableIO(io)

const fileSize = await io.size();
if (fileSize === 0) {
    // First format: ensure size and write both SBs
    await sbm.formatInitial({
        journalStart: WAL_Manager.J_START,
        epoch: BigInt(Date.now()),
    });
    wal.format(1073741824)
    await tio.formatInitial({ epoch: BigInt(Date.now()) })
    // wal.initFrom(WAL_Manager.J_START, WAL_Manager.J_START, 0n);
} else {
    // Load SB and init WAL tail/head/lsn
    const sb = await sbm.load();
    tio = await tio.load()
    wal.initFrom(Number(sb.jHead), Number(sb.jTail), sb.checkpointLSN);
}

// 3) Use EventRing
const t = new LSM(8, tio);
const er = new EventRing(t, wal, sbm);
if (wal.getUsed() > 0) {
    await t.recover(wal, er, sbm)
}

// await tio.popEntry()
for (let i = 0; i < 5; i++) {
    const array = new Uint32Array(10);

    er.dispatch({
        op: "set",
        key: Bun.randomUUIDv7(),
        value: crypto.getRandomValues(array).toString(),
        ts: 0,
        next: null,
    });
}
er.dispatch({
    op: "get",
    key: "alice",
    ts: 0,
    next: null,
    onComplete(r) {
        console.log("got alice:", r);
    },
});

// 4) Give it time to process (1 ms is often too short)
while (true) await er.runFor(10);

