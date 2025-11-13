import { WAL_Manager } from "./wal";
import type { ManifestEntry, ManifestPage } from "./types";
import { BLOCK, PREFIX, ENTRY_SIZE, HEADER_SIZE, CAP, MANIFEST_OFF, MANIFEST_RESULT, DatabaseError } from "./constants";

function encodeManifestEntry(me: ManifestEntry): Uint8Array {
    if (me.minPrefix.length !== PREFIX || me.maxPrefix.length !== PREFIX) {
        throw new DatabaseError(MANIFEST_RESULT.INVALID_PREFIX_SIZE, `prefixes must be exactly ${PREFIX} bytes`);
    }
    const buf = new Uint8Array(ENTRY_SIZE);
    const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    let o = 0;
    v.setUint16(o, me.level, true); o += 2;
    v.setUint16(o, 0, true); o += 2; // reserved
    v.setBigUint64(o, me.metaOff, true); o += 8;
    v.setUint32(o, me.metaLen, true); o += 4;
    buf.set(me.minPrefix, o); o += PREFIX;
    buf.set(me.maxPrefix, o); o += PREFIX;
    return buf;
}

export function encodeManifestPage(mp: ManifestPage): Uint8Array {
    if (mp.entries.length > CAP) {
        throw new DatabaseError(MANIFEST_RESULT.TOO_MANY_ENTRIES, `too many entries: ${mp.entries.length} > cap ${CAP}`);
    }
    const buf = new Uint8Array(BLOCK);
    const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    let o = 0;

    const version = mp.version ?? 1;

    // Header
    v.setUint16(o, version, true); o += 2;
    v.setUint16(o, 0, true); o += 2; // reserved
    v.setBigUint64(o, mp.epoch, true); o += 8;
    v.setUint16(o, mp.entries.length, true); o += 2;
    v.setUint16(o, 0, true); o += 2; // reserved

    // Entries
    for (const e of mp.entries) {
        const data = encodeManifestEntry(e);
        buf.set(data, o);
        o += ENTRY_SIZE; // advance!
    }

    // Remaining bytes are zero (already)
    return buf;
}

export function decodeManifestPage(buf: Uint8Array): ManifestPage {
    if (buf.length !== BLOCK) throw new DatabaseError(MANIFEST_RESULT.INVALID_PAGE_SIZE, `manifest page must be ${BLOCK} bytes`);

    const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    let o = 0;

    const version = v.getUint16(o, true); o += 2;
    o += 2; // reserved
    const epoch = v.getBigUint64(o, true); o += 8;
    const count = v.getUint16(o, true); o += 2;
    o += 2; // reserved

    // Empty/uninitialized page → return empty manifest
    const allZero =
        version === 0 && epoch === 0n && count === 0;
    if (allZero) return { epoch: 0n, version: 0, entries: [] };

    if (count > CAP) {
        throw new DatabaseError(MANIFEST_RESULT.COUNT_EXCEEDS_CAP, `manifest count ${count} exceeds cap ${CAP}`);
    }
    const need = HEADER_SIZE + count * ENTRY_SIZE;
    if (need > buf.length) {
        throw new DatabaseError(MANIFEST_RESULT.CORRUPT, `manifest corrupt: need ${need}, have ${buf.length}`);
    }

    const entries: ManifestEntry[] = [];
    for (let i = 0; i < count; i++) {
        const eOff = HEADER_SIZE + i * ENTRY_SIZE;
        const ev = new DataView(buf.buffer, buf.byteOffset + eOff, ENTRY_SIZE);
        let eo = 0;

        const level = ev.getUint16(eo, true); eo += 2;
        eo += 2; // reserved
        const metaOff = ev.getBigUint64(eo, true); eo += 8;
        const metaLen = ev.getUint32(eo, true); eo += 4;

        const minPrefix = buf.subarray(eOff + eo, eOff + eo + PREFIX); eo += PREFIX;
        const maxPrefix = buf.subarray(eOff + eo, eOff + eo + PREFIX); eo += PREFIX;

        entries.push({
            level,
            metaOff,
            metaLen,
            minPrefix: new Uint8Array(minPrefix),
            maxPrefix: new Uint8Array(maxPrefix),
        });
    }
    return { epoch, version, entries };
}