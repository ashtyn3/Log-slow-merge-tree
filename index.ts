import { FileIO } from "./file-manager";
import { SuperblockManager } from "./superblock";
import { WAL_Manager } from "./wal";
import { LSM } from "./lsm-tree";
import { EventRing } from "./event-ring";
import { TableIO, TableReader } from "./table";
import { extractSortKey16, log, LogLevel } from "./utils";
import { kWayMerger } from "./k_way_merge_heaper";


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

const t = new LSM(8);
const er = new EventRing(t, wal, tio, sbm);
if (wal.getUsed() > 0) {
    await t.recover(wal, sbm, er)
}

async function fill(a: number) {
    for (let i = 0; i < a; i++) {
        er.dispatch({
            op: "set",
            key: `${i}`,
            value: "hi",
            ts: 0,
            next: null,
        });
    }
    while (true) {
        await er.runFor(10);
    }
}

await fill(2)

let readers = []
for (const head of await tio.aggHeads(0)) {
    const tr = new TableReader(io, head)
    readers.push(tr)
}
// console.log(await tio.levelSize(0))
//
// for await (const b of kWayMerger(readers)) {
//     console.log(b)
// }

// console.log('manual read all:')
// for (const reader of readers) {
//     let cur = await reader.next();
//     while (cur) {
//         console.log(cur);
//         cur = await reader.next();
//     }
// }
