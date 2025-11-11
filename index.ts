import { FileIO } from "./file-manager";
import { SuperblockManager } from "./superblock";
import { WAL_Manager } from "./wal";
import { extractSortKey16, LSM, TableReader } from "./lsm-tree";
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

const t = new LSM(48);
const er = new EventRing(t, wal, tio, sbm);
if (wal.getUsed() > 0) {
    console.log("recovering")
    await t.recover(wal, sbm, er)
}

async function fill(a: number) {
    for (let i = 0; i < a; i++) {
        const array = new Uint8Array(10);
        er.dispatch({
            op: "set",
            key: crypto.getRandomValues(array).toBase64(),
            value: "hi",
            ts: 0,
            next: null,
        });
    }
    while (true) await er.runFor(10);
}

// await fill(50)

for (const head of await tio.aggHeads()) {
    console.log()
    console.log("====")
    console.log(head.table.id)
    console.log("====")
    const tr = new TableReader(io, tio, head)
    while (true) {
        const kv = await tr.next()
        if (kv === null) break;
        console.log(kv.key.toHex(), new TextDecoder().decode(kv.value))
        console.log("\tsort key:", extractSortKey16(kv.key).toHex())
    }
    console.log("entry count:", head.table.entryCount)
    console.log("max:", head.table.maxKey.toHex(), "min:", head.table.minKey.toHex())
}
