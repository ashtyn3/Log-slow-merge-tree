// file-manager.ts
import { open as openFile } from "node:fs/promises";

// constants.ts
var BLOCK = 4096;
var SB_A_OFF = 0;
var SB_B_OFF = BLOCK;
var J_START = 2 * BLOCK;
var J_LENGTH = 256 * BLOCK;
var MANIFEST_OFF = J_START + J_LENGTH;
var OP = { set: 1, del: 2, get: 3, check: 4 };
var OP_INV = { 1: "set", 2: "del", 3: "get", 4: "check" };
var MAX_INFLIGHT = 8;
var PREFIX = 16;
var ENTRY_SIZE = 2 + 2 + 8 + 4 + PREFIX + PREFIX;
var HEADER_SIZE = 2 + 2 + 8 + 2 + 2;
var CAP = Math.floor((BLOCK - HEADER_SIZE) / ENTRY_SIZE);
var WAL_RESULT = {
  OK: 0,
  WAL_FULL: 1001,
  LSN_NOT_FOUND: 1002
};
var TABLE_RESULT = {
  OK: 0,
  INVALID_KEY_SIZE: 2001,
  TRUNCATED_ID: 2002,
  TRUNCATED_EXTENTS: 2003,
  MANIFEST_FULL: 2004,
  NEEDS_COMPACTION: 2005,
  BROKEN_TABLE_SIZE: 2006,
  ENTRY_NOT_EXIST: 2007,
  INVALID_PREFIX_SIZE: 2008
};
var MANIFEST_RESULT = {
  OK: 0,
  INVALID_PREFIX_SIZE: 3001,
  TOO_MANY_ENTRIES: 3002,
  INVALID_PAGE_SIZE: 3003,
  COUNT_EXCEEDS_CAP: 3004,
  CORRUPT: 3005
};
var FILE_RESULT = {
  OK: 0,
  SHORT_READ: 4001
};
var SUPERBLOCK_RESULT = {
  OK: 0,
  NO_VALID_SUPERBLOCKS: 5001,
  NOT_INITIALIZED: 5002
};
var CLOCK_RESULT = {
  OK: 0,
  CORRUPTED_EPOCH: 6001,
  BROKEN_CLOCK_STATE: 6002
};

class DatabaseError extends Error {
  code;
  constructor(code, message) {
    super(message);
    this.code = code;
    this.name = "DatabaseError";
  }
}
var LOG_LEVEL = 1;

// file-manager.ts
var alignUp = (n, blk = BLOCK) => n + (blk - 1) & ~(blk - 1);

class FileIO {
  path;
  fh;
  constructor(path) {
    this.path = path;
  }
  async open(flag = "r+") {
    this.fh = await openFile(this.path, flag);
  }
  async close() {
    if (this.fh)
      await this.fh.close();
  }
  async size() {
    const s = await this.fh.stat();
    return s.size;
  }
  async ensureSize(bytes) {
    const cur = await this.size();
    if (cur >= bytes)
      return;
    await this.fh.truncate(bytes);
    await this.fh.sync();
  }
  async write(offset, buf) {
    await this.fh.write(buf, 0, buf.length, offset);
  }
  async writev(offset, bufs) {
    let off = offset;
    for (const b of bufs) {
      await this.fh.write(b, 0, b.length, off);
      off += b.length;
    }
  }
  async readExact(offset, len) {
    const out = new Uint8Array(len);
    let filled = 0;
    while (filled < len) {
      const { bytesRead } = await this.fh.read(out, filled, len - filled, offset + filled);
      if (bytesRead === 0)
        throw new DatabaseError(FILE_RESULT.SHORT_READ, "short read");
      filled += bytesRead;
    }
    return out;
  }
  async read(offset, len) {
    const out = new Uint8Array(len);
    const { bytesRead } = await this.fh.read(out, 0, len, offset);
    return out.subarray(0, bytesRead);
  }
  async fsync() {
    await this.fh.sync();
  }
}

// superblock.ts
function encodeSB(sb) {
  const buf = new Uint8Array(BLOCK);
  const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  let o = 0;
  v.setUint16(o, sb.version, true);
  o += 2;
  v.setUint16(o, sb.blockSize, true);
  o += 2;
  v.setBigUint64(o, sb.epoch, true);
  o += 8;
  v.setBigUint64(o, sb.checkpointLSN, true);
  o += 8;
  v.setBigUint64(o, sb.jHead, true);
  o += 8;
  v.setBigUint64(o, sb.jTail, true);
  o += 8;
  return buf;
}
function decodeSB(buf) {
  if (buf.length < BLOCK)
    return null;
  const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  let o = 0;
  const version = v.getUint16(o, true);
  o += 2;
  const blockSize = v.getUint16(o, true);
  o += 2;
  const epoch = v.getBigUint64(o, true);
  o += 8;
  const checkpointLSN = v.getBigUint64(o, true);
  o += 8;
  const jHead = v.getBigUint64(o, true);
  o += 8;
  const jTail = v.getBigUint64(o, true);
  o += 8;
  if (blockSize !== BLOCK || version === 0)
    return null;
  return { version, blockSize, epoch, checkpointLSN, jHead, jTail };
}

class SuperblockManager {
  file;
  active = "A";
  sb = null;
  constructor(file) {
    this.file = file;
  }
  async formatInitial({
    journalStart,
    epoch = 1n
  }) {
    const base = {
      version: 1,
      blockSize: BLOCK,
      epoch,
      checkpointLSN: 0n,
      jHead: BigInt(journalStart),
      jTail: BigInt(journalStart)
    };
    const buf = encodeSB(base);
    await this.file.write(SB_A_OFF, buf);
    await this.file.write(SB_B_OFF, buf);
    await this.file.fsync();
    this.active = "A";
    this.sb = base;
  }
  async load() {
    const a = await this.file.read(SB_A_OFF, BLOCK);
    const b = await this.file.read(SB_B_OFF, BLOCK);
    const sa = decodeSB(a);
    const sb = decodeSB(b);
    if (!sa && !sb)
      throw new DatabaseError(SUPERBLOCK_RESULT.NO_VALID_SUPERBLOCKS, "no valid superblocks");
    let chosen;
    if (sa && sb) {
      if (sb.epoch > sa.epoch) {
        chosen = sb;
        this.active = "B";
      } else if (sa.epoch > sb.epoch) {
        chosen = sa;
        this.active = "A";
      } else {
        chosen = sb;
        this.active = "B";
      }
    } else {
      chosen = sa ?? sb;
      this.active = sa ? "A" : "B";
    }
    this.sb = chosen;
    return chosen;
  }
  async checkpoint(update) {
    if (!this.sb)
      throw new DatabaseError(SUPERBLOCK_RESULT.NOT_INITIALIZED, "call load() or formatInitial() first");
    const next = {
      version: this.sb.version,
      blockSize: BLOCK,
      epoch: update.epoch ?? this.sb.epoch,
      checkpointLSN: update.checkpointLSN ?? this.sb.checkpointLSN,
      jHead: update.jHead ?? this.sb.jHead,
      jTail: update.jTail ?? this.sb.jTail
    };
    const buf = encodeSB(next);
    const targetOff = this.active === "A" ? SB_B_OFF : SB_A_OFF;
    await this.file.write(targetOff, buf);
    await this.file.fsync();
    this.active = this.active === "A" ? "B" : "A";
    this.sb = next;
  }
  current() {
    return this.sb;
  }
}

// utils.ts
import { createHash } from "node:crypto";
function extractSortKey16(bytes) {
  const h = createHash("blake2b512").update(bytes).digest();
  return new Uint8Array(h.buffer, h.byteOffset, 16);
}
function cmp16(a, b) {
  for (let i = 0;i < 16; i++) {
    const d = a[i] - b[i];
    if (d !== 0)
      return d;
  }
  return 0;
}
function lt16(a, b) {
  return cmp16(a, b) < 0;
}
function gt16(a, b) {
  return cmp16(a, b) > 0;
}
var LogLevel;
((LogLevel2) => {
  LogLevel2[LogLevel2["debug"] = 0] = "debug";
  LogLevel2[LogLevel2["info"] = 1] = "info";
  LogLevel2[LogLevel2["warn"] = 2] = "warn";
  LogLevel2[LogLevel2["err"] = 3] = "err";
})(LogLevel ||= {});
function log(level, message, ...args) {
  if (level >= LOG_LEVEL) {
    const levelStr = LogLevel[level].toUpperCase();
    const timestamp = new Date().toISOString();
    console.error(`${timestamp} ${levelStr}: ${message}`, ...args);
  }
}

// wal.ts
class WAL_Manager {
  file;
  lsnToEnd = new Map;
  alignUp(n, a = 8) {
    return n + (a - 1) & ~(a - 1);
  }
  lsn = 0n;
  head;
  tail;
  jStart;
  jEnd;
  minHdr = 8 + 1 + 4 + 4;
  OP_PAD = 0;
  constructor(file, opts = { journalBytes: J_LENGTH }) {
    this.file = file;
    this.jStart = opts.jStart ?? J_START;
    this.jEnd = this.jStart + opts.journalBytes;
    this.head = this.jStart;
    this.tail = this.jStart;
  }
  async format(totalSizeBytes) {
    await this.file.ensureSize(totalSizeBytes);
    this.head = this.jStart;
    this.tail = this.jStart;
    this.lsn = 0n;
  }
  async initFrom(head, tail, lastLSN) {
    this.head = head;
    this.tail = tail;
    this.lsn = lastLSN;
    for (const e of await this.scan(this.head, this.tail)) {
      this.lsnToEnd.set(e.lsn, this.head + e.off);
    }
  }
  encodeRecord(lsn, op, key, value) {
    const enc = new TextEncoder;
    const kb = enc.encode(key);
    const vb = value !== undefined ? enc.encode(value) : new Uint8Array(0);
    const raw = new Uint8Array(this.minHdr + kb.length + vb.length);
    const v = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
    let o = 0;
    v.setBigUint64(o, lsn, true);
    o += 8;
    v.setUint8(o, op);
    o += 1;
    v.setUint32(o, kb.length, true);
    o += 4;
    v.setUint32(o, vb.length, true);
    o += 4;
    raw.set(kb, o);
    o += kb.length;
    raw.set(vb, o);
    const out = new Uint8Array(this.alignUp(raw.length));
    out.set(raw, 0);
    return out;
  }
  encodePad(lsn) {
    const raw = new Uint8Array(this.minHdr);
    const v = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
    let o = 0;
    v.setBigUint64(o, lsn, true);
    o += 8;
    v.setUint8(o, this.OP_PAD);
    o += 1;
    v.setUint32(o, 0, true);
    o += 4;
    v.setUint32(o, 0, true);
    o += 4;
    const out = new Uint8Array(this.alignUp(raw.length));
    out.set(raw, 0);
    return out;
  }
  getUsed() {
    return this.tail >= this.head ? this.tail - this.head : this.jEnd - this.head + (this.tail - this.jStart);
  }
  async appendMany(items) {
    const toNumOp = (op) => OP[op];
    let next = this.lsn;
    const recs = items.map((it) => {
      next += 1n;
      return {
        lsn: next,
        buf: this.encodeRecord(next, toNumOp(it.op), it.key, it.value)
      };
    });
    const batchBytes = recs.reduce((s, r) => s + r.buf.length, 0);
    const padBytes = this.alignUp(this.minHdr);
    const regionBytes = this.jEnd - this.jStart;
    const used = this.getUsed();
    const free = regionBytes - used;
    const needsWrap = this.tail + batchBytes > this.jEnd;
    const needTotal = batchBytes + (needsWrap ? padBytes : 0);
    if (free < needTotal) {
      log(3 /* err */, "WAL full", { free, need: needTotal, batch: batchBytes, wrap: needsWrap });
      throw new DatabaseError(WAL_RESULT.WAL_FULL, `WAL full: free=${free} (${free / BLOCK | 0} blocks) ` + `need=${needTotal} (${needTotal / BLOCK | 0} blocks) ` + `(batch=${batchBytes} wrap=${needsWrap})`);
    }
    if (needsWrap) {
      const pad = this.encodePad(this.lsn);
      await this.file.write(this.tail, pad);
      this.tail = this.jStart;
    }
    let off = this.tail;
    for (const r of recs) {
      await this.file.write(off, r.buf);
      off += r.buf.length;
      const normEnd = off === this.jEnd ? this.jStart : off;
      this.lsnToEnd.set(r.lsn, normEnd);
    }
    this.tail = off === this.jEnd ? this.jStart : off;
    this.lsn = next;
    log(0 /* debug */, "Appended batch to WAL", { count: items.length, lastLSN: next });
    await this.file.fsync();
    return next;
  }
  async checkpoint(lsn, sbm) {
    const offset = this.lsnToEnd.get(lsn);
    if (offset === undefined) {
      log(3 /* err */, "Checkpoint failed: LSN not found", { lsn });
      throw new DatabaseError(WAL_RESULT.LSN_NOT_FOUND, `cannot checkpoint: LSN ${lsn} not found`);
    }
    this.head = offset;
    for (const k of this.lsnToEnd.keys()) {
      if (k <= offset)
        this.lsnToEnd.delete(k);
    }
    log(1 /* info */, "Checkpointed WAL", { lsn });
    await sbm.checkpoint({
      checkpointLSN: lsn,
      jHead: BigInt(this.head),
      jTail: BigInt(this.tail)
    });
  }
  decodeAt(view, offset) {
    const end = view.byteLength;
    if (offset + this.minHdr > end)
      return null;
    const dec = new TextDecoder;
    let o = offset;
    const lsn = view.getBigUint64(o, true);
    o += 8;
    const op = view.getUint8(o);
    o += 1;
    const klen = view.getUint32(o, true);
    o += 4;
    const vlen = view.getUint32(o, true);
    o += 4;
    if (op === this.OP_PAD) {
      const next2 = this.alignUp(o);
      return { kind: "pad", next: next2 };
    }
    const need = o + klen + vlen;
    if (need > end)
      return null;
    const keyBytes = new Uint8Array(view.buffer, view.byteOffset + o, klen);
    o += klen;
    const valBytes = new Uint8Array(view.buffer, view.byteOffset + o, vlen);
    o += vlen;
    const next = this.alignUp(o);
    return {
      kind: "entry",
      next,
      lsn,
      op,
      key: dec.decode(keyBytes),
      value: vlen > 0 ? dec.decode(valBytes) : undefined
    };
  }
  async scan(from, maxBytes) {
    const buf = await this.file.read(from, maxBytes);
    const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    const out = [];
    let off = 0;
    while (off < view.byteLength) {
      const r = this.decodeAt(view, off);
      if (!r)
        break;
      if (r.kind === "pad") {
        off = r.next;
        continue;
      }
      out.push({ lsn: r.lsn, op: r.op, key: r.key, value: r.value, off });
      off = r.next;
    }
    return out;
  }
  getHead() {
    return this.head;
  }
  getTail() {
    return this.tail;
  }
  getLastLSN() {
    return this.lsn;
  }
  isDirty() {
    if (this.getUsed() > 0)
      return true;
    return false;
  }
  advanceHeadTo(offset) {
    this.head = offset;
  }
}

// b_tree/b_tree.ts
function defaultComparator(a, b) {
  if (Number.isFinite(a) && Number.isFinite(b)) {
    return a - b;
  }
  let ta = typeof a;
  let tb = typeof b;
  if (ta !== tb) {
    return ta < tb ? -1 : 1;
  }
  if (ta === "object") {
    if (a === null)
      return b === null ? 0 : -1;
    else if (b === null)
      return 1;
    a = a.valueOf();
    b = b.valueOf();
    ta = typeof a;
    tb = typeof b;
    if (ta !== tb) {
      return ta < tb ? -1 : 1;
    }
  }
  if (a < b)
    return -1;
  if (a > b)
    return 1;
  if (a === b)
    return 0;
  if (Number.isNaN(a))
    return Number.isNaN(b) ? 0 : -1;
  else if (Number.isNaN(b))
    return 1;
  return Array.isArray(a) ? 0 : Number.NaN;
}
class BTree {
  _root = EmptyLeaf;
  _size = 0;
  _maxNodeSize;
  _compare;
  constructor(entries, compare, maxNodeSize) {
    this._maxNodeSize = maxNodeSize >= 4 ? Math.min(maxNodeSize, 256) : 32;
    this._compare = compare || defaultComparator;
    if (entries)
      this.setPairs(entries);
  }
  get size() {
    return this._size;
  }
  get length() {
    return this._size;
  }
  get isEmpty() {
    return this._size === 0;
  }
  clear() {
    this._root = EmptyLeaf;
    this._size = 0;
  }
  forEach(callback, thisArg) {
    if (thisArg !== undefined)
      callback = callback.bind(thisArg);
    return this.forEachPair((k, v) => callback(v, k, this));
  }
  forEachPair(callback, initialCounter) {
    var low = this.minKey(), high = this.maxKey();
    return this.forRange(low, high, true, callback, initialCounter);
  }
  get(key, defaultValue) {
    return this._root.get(key, defaultValue, this);
  }
  set(key, value, overwrite) {
    if (this._root.isShared)
      this._root = this._root.clone();
    var result = this._root.set(key, value, overwrite, this);
    if (result === true || result === false)
      return result;
    this._root = new BNodeInternal([this._root, result]);
    return true;
  }
  has(key) {
    return this.forRange(key, key, true, undefined) !== 0;
  }
  delete(key) {
    return this.editRange(key, key, true, DeleteRange) !== 0;
  }
  with(key, value, overwrite) {
    let nu = this.clone();
    return nu.set(key, value, overwrite) || overwrite ? nu : this;
  }
  withPairs(pairs, overwrite) {
    let nu = this.clone();
    return nu.setPairs(pairs, overwrite) !== 0 || overwrite ? nu : this;
  }
  withKeys(keys, returnThisIfUnchanged) {
    let nu = this.clone(), changed = false;
    for (var i = 0;i < keys.length; i++)
      changed = nu.set(keys[i], undefined, false) || changed;
    return returnThisIfUnchanged && !changed ? this : nu;
  }
  without(key, returnThisIfUnchanged) {
    return this.withoutRange(key, key, true, returnThisIfUnchanged);
  }
  withoutKeys(keys, returnThisIfUnchanged) {
    let nu = this.clone();
    return nu.deleteKeys(keys) || !returnThisIfUnchanged ? nu : this;
  }
  withoutRange(low, high, includeHigh, returnThisIfUnchanged) {
    let nu = this.clone();
    if (nu.deleteRange(low, high, includeHigh) === 0 && returnThisIfUnchanged)
      return this;
    return nu;
  }
  filter(callback, returnThisIfUnchanged) {
    var nu = this.greedyClone();
    var del;
    nu.editAll((k, v, i) => {
      if (!callback(k, v, i))
        return del = Delete;
    });
    if (!del && returnThisIfUnchanged)
      return this;
    return nu;
  }
  mapValues(callback) {
    var tmp = {};
    var nu = this.greedyClone();
    nu.editAll((k, v, i) => {
      return tmp.value = callback(v, k, i), tmp;
    });
    return nu;
  }
  reduce(callback, initialValue) {
    let i = 0, p = initialValue;
    var it = this.entries(this.minKey(), ReusedArray), next;
    while (!(next = it.next()).done)
      p = callback(p, next.value, i++, this);
    return p;
  }
  entries(lowestKey, reusedArray) {
    var info = this.findPath(lowestKey);
    if (info === undefined)
      return iterator();
    var { nodequeue, nodeindex, leaf } = info;
    var state = reusedArray !== undefined ? 1 : 0;
    var i = lowestKey === undefined ? -1 : leaf.indexOf(lowestKey, 0, this._compare) - 1;
    return iterator(() => {
      jump:
        for (;; ) {
          switch (state) {
            case 0:
              if (++i < leaf.keys.length)
                return { done: false, value: [leaf.keys[i], leaf.values[i]] };
              state = 2;
              continue;
            case 1:
              if (++i < leaf.keys.length) {
                reusedArray[0] = leaf.keys[i], reusedArray[1] = leaf.values[i];
                return { done: false, value: reusedArray };
              }
              state = 2;
            case 2:
              for (var level = -1;; ) {
                if (++level >= nodequeue.length) {
                  state = 3;
                  continue jump;
                }
                if (++nodeindex[level] < nodequeue[level].length)
                  break;
              }
              for (;level > 0; level--) {
                nodequeue[level - 1] = nodequeue[level][nodeindex[level]].children;
                nodeindex[level - 1] = 0;
              }
              leaf = nodequeue[0][nodeindex[0]];
              i = -1;
              state = reusedArray !== undefined ? 1 : 0;
              continue;
            case 3:
              return { done: true, value: undefined };
          }
        }
    });
  }
  entriesReversed(highestKey, reusedArray, skipHighest) {
    if (highestKey === undefined) {
      highestKey = this.maxKey();
      skipHighest = undefined;
      if (highestKey === undefined)
        return iterator();
    }
    var { nodequeue, nodeindex, leaf } = this.findPath(highestKey) || this.findPath(this.maxKey());
    check(!nodequeue[0] || leaf === nodequeue[0][nodeindex[0]], "wat!");
    var i = leaf.indexOf(highestKey, 0, this._compare);
    if (!skipHighest && i < leaf.keys.length && this._compare(leaf.keys[i], highestKey) <= 0)
      i++;
    var state = reusedArray !== undefined ? 1 : 0;
    return iterator(() => {
      jump:
        for (;; ) {
          switch (state) {
            case 0:
              if (--i >= 0)
                return { done: false, value: [leaf.keys[i], leaf.values[i]] };
              state = 2;
              continue;
            case 1:
              if (--i >= 0) {
                reusedArray[0] = leaf.keys[i], reusedArray[1] = leaf.values[i];
                return { done: false, value: reusedArray };
              }
              state = 2;
            case 2:
              for (var level = -1;; ) {
                if (++level >= nodequeue.length) {
                  state = 3;
                  continue jump;
                }
                if (--nodeindex[level] >= 0)
                  break;
              }
              for (;level > 0; level--) {
                nodequeue[level - 1] = nodequeue[level][nodeindex[level]].children;
                nodeindex[level - 1] = nodequeue[level - 1].length - 1;
              }
              leaf = nodequeue[0][nodeindex[0]];
              i = leaf.keys.length;
              state = reusedArray !== undefined ? 1 : 0;
              continue;
            case 3:
              return { done: true, value: undefined };
          }
        }
    });
  }
  findPath(key) {
    var nextnode = this._root;
    var nodequeue, nodeindex;
    if (nextnode.isLeaf) {
      nodequeue = EmptyArray, nodeindex = EmptyArray;
    } else {
      nodequeue = [], nodeindex = [];
      for (var d = 0;!nextnode.isLeaf; d++) {
        nodequeue[d] = nextnode.children;
        nodeindex[d] = key === undefined ? 0 : nextnode.indexOf(key, 0, this._compare);
        if (nodeindex[d] >= nodequeue[d].length)
          return;
        nextnode = nodequeue[d][nodeindex[d]];
      }
      nodequeue.reverse();
      nodeindex.reverse();
    }
    return { nodequeue, nodeindex, leaf: nextnode };
  }
  keys(firstKey) {
    var it = this.entries(firstKey, ReusedArray);
    return iterator(() => {
      var n = it.next();
      if (n.value)
        n.value = n.value[0];
      return n;
    });
  }
  values(firstKey) {
    var it = this.entries(firstKey, ReusedArray);
    return iterator(() => {
      var n = it.next();
      if (n.value)
        n.value = n.value[1];
      return n;
    });
  }
  get maxNodeSize() {
    return this._maxNodeSize;
  }
  minKey() {
    return this._root.minKey();
  }
  maxKey() {
    return this._root.maxKey();
  }
  clone() {
    this._root.isShared = true;
    var result = new BTree(undefined, this._compare, this._maxNodeSize);
    result._root = this._root;
    result._size = this._size;
    return result;
  }
  greedyClone(force) {
    var result = new BTree(undefined, this._compare, this._maxNodeSize);
    result._root = this._root.greedyClone(force);
    result._size = this._size;
    return result;
  }
  toArray(maxLength = 2147483647) {
    let min = this.minKey(), max = this.maxKey();
    if (min !== undefined)
      return this.getRange(min, max, true, maxLength);
    return [];
  }
  keysArray() {
    var results = [];
    this._root.forRange(this.minKey(), this.maxKey(), true, false, this, 0, (k, v) => {
      results.push(k);
    });
    return results;
  }
  valuesArray() {
    var results = [];
    this._root.forRange(this.minKey(), this.maxKey(), true, false, this, 0, (k, v) => {
      results.push(v);
    });
    return results;
  }
  toString() {
    return this.toArray().toString();
  }
  setIfNotPresent(key, value) {
    return this.set(key, value, false);
  }
  nextHigherPair(key, reusedArray) {
    reusedArray = reusedArray || [];
    if (key === undefined) {
      return this._root.minPair(reusedArray);
    }
    return this._root.getPairOrNextHigher(key, this._compare, false, reusedArray);
  }
  nextHigherKey(key) {
    var p = this.nextHigherPair(key, ReusedArray);
    return p && p[0];
  }
  nextLowerPair(key, reusedArray) {
    reusedArray = reusedArray || [];
    if (key === undefined) {
      return this._root.maxPair(reusedArray);
    }
    return this._root.getPairOrNextLower(key, this._compare, false, reusedArray);
  }
  nextLowerKey(key) {
    var p = this.nextLowerPair(key, ReusedArray);
    return p && p[0];
  }
  getPairOrNextLower(key, reusedArray) {
    return this._root.getPairOrNextLower(key, this._compare, true, reusedArray || []);
  }
  getPairOrNextHigher(key, reusedArray) {
    return this._root.getPairOrNextHigher(key, this._compare, true, reusedArray || []);
  }
  changeIfPresent(key, value) {
    return this.editRange(key, key, true, (k, v) => ({ value })) !== 0;
  }
  getRange(low, high, includeHigh, maxLength = 67108863) {
    var results = [];
    this._root.forRange(low, high, includeHigh, false, this, 0, (k, v) => {
      results.push([k, v]);
      return results.length > maxLength ? Break : undefined;
    });
    return results;
  }
  setPairs(pairs, overwrite) {
    var added = 0;
    for (var i = 0;i < pairs.length; i++)
      if (this.set(pairs[i][0], pairs[i][1], overwrite))
        added++;
    return added;
  }
  forRange(low, high, includeHigh, onFound, initialCounter) {
    var r = this._root.forRange(low, high, includeHigh, false, this, initialCounter || 0, onFound);
    return typeof r === "number" ? r : r.break;
  }
  editRange(low, high, includeHigh, onFound, initialCounter) {
    var root = this._root;
    if (root.isShared)
      this._root = root = root.clone();
    try {
      var r = root.forRange(low, high, includeHigh, true, this, initialCounter || 0, onFound);
      return typeof r === "number" ? r : r.break;
    } finally {
      let isShared;
      while (root.keys.length <= 1 && !root.isLeaf) {
        isShared ||= root.isShared;
        this._root = root = root.keys.length === 0 ? EmptyLeaf : root.children[0];
      }
      if (isShared) {
        root.isShared = true;
      }
    }
  }
  editAll(onFound, initialCounter) {
    return this.editRange(this.minKey(), this.maxKey(), true, onFound, initialCounter);
  }
  deleteRange(low, high, includeHigh) {
    return this.editRange(low, high, includeHigh, DeleteRange);
  }
  deleteKeys(keys) {
    for (var i = 0, r = 0;i < keys.length; i++)
      if (this.delete(keys[i]))
        r++;
    return r;
  }
  get height() {
    let node = this._root;
    let height = -1;
    while (node) {
      height++;
      node = node.isLeaf ? undefined : node.children[0];
    }
    return height;
  }
  freeze() {
    var t = this;
    t.clear = t.set = t.editRange = function() {
      throw new Error("Attempted to modify a frozen BTree");
    };
  }
  unfreeze() {
    delete this.clear;
    delete this.set;
    delete this.editRange;
  }
  get isFrozen() {
    return this.hasOwnProperty("editRange");
  }
  checkValid() {
    var size = this._root.checkValid(0, this, 0);
    check(size === this.size, "size mismatch: counted ", size, "but stored", this.size);
  }
}
if (Symbol && Symbol.iterator)
  BTree.prototype[Symbol.iterator] = BTree.prototype.entries;
BTree.prototype.where = BTree.prototype.filter;
BTree.prototype.setRange = BTree.prototype.setPairs;
BTree.prototype.add = BTree.prototype.set;
function iterator(next = () => ({ done: true, value: undefined })) {
  var result = { next };
  if (Symbol && Symbol.iterator)
    result[Symbol.iterator] = function() {
      return this;
    };
  return result;
}

class BNode {
  keys;
  values;
  isShared;
  get isLeaf() {
    return this.children === undefined;
  }
  constructor(keys = [], values) {
    this.keys = keys;
    this.values = values || undefVals;
    this.isShared = undefined;
  }
  maxKey() {
    return this.keys[this.keys.length - 1];
  }
  indexOf(key, failXor, cmp) {
    const keys = this.keys;
    var lo = 0, hi = keys.length, mid = hi >> 1;
    while (lo < hi) {
      var c = cmp(keys[mid], key);
      if (c < 0)
        lo = mid + 1;
      else if (c > 0)
        hi = mid;
      else if (c === 0)
        return mid;
      else {
        if (key === key)
          return keys.length;
        else
          throw new Error("BTree: NaN was used as a key");
      }
      mid = lo + hi >> 1;
    }
    return mid ^ failXor;
  }
  minKey() {
    return this.keys[0];
  }
  minPair(reusedArray) {
    if (this.keys.length === 0)
      return;
    reusedArray[0] = this.keys[0];
    reusedArray[1] = this.values[0];
    return reusedArray;
  }
  maxPair(reusedArray) {
    if (this.keys.length === 0)
      return;
    const lastIndex = this.keys.length - 1;
    reusedArray[0] = this.keys[lastIndex];
    reusedArray[1] = this.values[lastIndex];
    return reusedArray;
  }
  clone() {
    var v = this.values;
    return new BNode(this.keys.slice(0), v === undefVals ? v : v.slice(0));
  }
  greedyClone(force) {
    return this.isShared && !force ? this : this.clone();
  }
  get(key, defaultValue, tree) {
    var i = this.indexOf(key, -1, tree._compare);
    return i < 0 ? defaultValue : this.values[i];
  }
  getPairOrNextLower(key, compare, inclusive, reusedArray) {
    var i = this.indexOf(key, -1, compare);
    const indexOrLower = i < 0 ? ~i - 1 : inclusive ? i : i - 1;
    if (indexOrLower >= 0) {
      reusedArray[0] = this.keys[indexOrLower];
      reusedArray[1] = this.values[indexOrLower];
      return reusedArray;
    }
    return;
  }
  getPairOrNextHigher(key, compare, inclusive, reusedArray) {
    var i = this.indexOf(key, -1, compare);
    const indexOrLower = i < 0 ? ~i : inclusive ? i : i + 1;
    const keys = this.keys;
    if (indexOrLower < keys.length) {
      reusedArray[0] = keys[indexOrLower];
      reusedArray[1] = this.values[indexOrLower];
      return reusedArray;
    }
    return;
  }
  checkValid(depth, tree, baseIndex) {
    var kL = this.keys.length, vL = this.values.length;
    check(this.values === undefVals ? kL <= vL : kL === vL, "keys/values length mismatch: depth", depth, "with lengths", kL, vL, "and baseIndex", baseIndex);
    check(depth == 0 || kL > 0, "empty leaf at depth", depth, "and baseIndex", baseIndex);
    return kL;
  }
  set(key, value, overwrite, tree) {
    var i = this.indexOf(key, -1, tree._compare);
    if (i < 0) {
      i = ~i;
      tree._size++;
      if (this.keys.length < tree._maxNodeSize) {
        return this.insertInLeaf(i, key, value, tree);
      } else {
        var newRightSibling = this.splitOffRightSide(), target = this;
        if (i > this.keys.length) {
          i -= this.keys.length;
          target = newRightSibling;
        }
        target.insertInLeaf(i, key, value, tree);
        return newRightSibling;
      }
    } else {
      if (overwrite !== false) {
        if (value !== undefined)
          this.reifyValues();
        this.keys[i] = key;
        this.values[i] = value;
      }
      return false;
    }
  }
  reifyValues() {
    if (this.values === undefVals)
      return this.values = this.values.slice(0, this.keys.length);
    return this.values;
  }
  insertInLeaf(i, key, value, tree) {
    this.keys.splice(i, 0, key);
    if (this.values === undefVals) {
      while (undefVals.length < tree._maxNodeSize)
        undefVals.push(undefined);
      if (value === undefined) {
        return true;
      } else {
        this.values = undefVals.slice(0, this.keys.length - 1);
      }
    }
    this.values.splice(i, 0, value);
    return true;
  }
  takeFromRight(rhs) {
    var v = this.values;
    if (rhs.values === undefVals) {
      if (v !== undefVals)
        v.push(undefined);
    } else {
      v = this.reifyValues();
      v.push(rhs.values.shift());
    }
    this.keys.push(rhs.keys.shift());
  }
  takeFromLeft(lhs) {
    var v = this.values;
    if (lhs.values === undefVals) {
      if (v !== undefVals)
        v.unshift(undefined);
    } else {
      v = this.reifyValues();
      v.unshift(lhs.values.pop());
    }
    this.keys.unshift(lhs.keys.pop());
  }
  splitOffRightSide() {
    var half = this.keys.length >> 1, keys = this.keys.splice(half);
    var values = this.values === undefVals ? undefVals : this.values.splice(half);
    return new BNode(keys, values);
  }
  forRange(low, high, includeHigh, editMode, tree, count, onFound) {
    var cmp = tree._compare;
    var iLow, iHigh;
    if (high === low) {
      if (!includeHigh)
        return count;
      iHigh = (iLow = this.indexOf(low, -1, cmp)) + 1;
      if (iLow < 0)
        return count;
    } else {
      iLow = this.indexOf(low, 0, cmp);
      iHigh = this.indexOf(high, -1, cmp);
      if (iHigh < 0)
        iHigh = ~iHigh;
      else if (includeHigh === true)
        iHigh++;
    }
    var keys = this.keys, values = this.values;
    if (onFound !== undefined) {
      for (var i = iLow;i < iHigh; i++) {
        var key = keys[i];
        var result = onFound(key, values[i], count++);
        if (result !== undefined) {
          if (editMode === true) {
            if (key !== keys[i] || this.isShared === true)
              throw new Error("BTree illegally changed or cloned in editRange");
            if (result.delete) {
              this.keys.splice(i, 1);
              if (this.values !== undefVals)
                this.values.splice(i, 1);
              tree._size--;
              i--;
              iHigh--;
            } else if (result.hasOwnProperty("value")) {
              values[i] = result.value;
            }
          }
          if (result.break !== undefined)
            return result;
        }
      }
    } else
      count += iHigh - iLow;
    return count;
  }
  mergeSibling(rhs, _) {
    this.keys.push.apply(this.keys, rhs.keys);
    if (this.values === undefVals) {
      if (rhs.values === undefVals)
        return;
      this.values = this.values.slice(0, this.keys.length);
    }
    this.values.push.apply(this.values, rhs.reifyValues());
  }
}

class BNodeInternal extends BNode {
  children;
  constructor(children, keys) {
    if (!keys) {
      keys = [];
      for (var i = 0;i < children.length; i++)
        keys[i] = children[i].maxKey();
    }
    super(keys);
    this.children = children;
  }
  clone() {
    var children = this.children.slice(0);
    for (var i = 0;i < children.length; i++)
      children[i].isShared = true;
    return new BNodeInternal(children, this.keys.slice(0));
  }
  greedyClone(force) {
    if (this.isShared && !force)
      return this;
    var nu = new BNodeInternal(this.children.slice(0), this.keys.slice(0));
    for (var i = 0;i < nu.children.length; i++)
      nu.children[i] = nu.children[i].greedyClone(force);
    return nu;
  }
  minKey() {
    return this.children[0].minKey();
  }
  minPair(reusedArray) {
    return this.children[0].minPair(reusedArray);
  }
  maxPair(reusedArray) {
    return this.children[this.children.length - 1].maxPair(reusedArray);
  }
  get(key, defaultValue, tree) {
    var i = this.indexOf(key, 0, tree._compare), children = this.children;
    return i < children.length ? children[i].get(key, defaultValue, tree) : undefined;
  }
  getPairOrNextLower(key, compare, inclusive, reusedArray) {
    var i = this.indexOf(key, 0, compare), children = this.children;
    if (i >= children.length)
      return this.maxPair(reusedArray);
    const result = children[i].getPairOrNextLower(key, compare, inclusive, reusedArray);
    if (result === undefined && i > 0) {
      return children[i - 1].maxPair(reusedArray);
    }
    return result;
  }
  getPairOrNextHigher(key, compare, inclusive, reusedArray) {
    var i = this.indexOf(key, 0, compare), children = this.children, length = children.length;
    if (i >= length)
      return;
    const result = children[i].getPairOrNextHigher(key, compare, inclusive, reusedArray);
    if (result === undefined && i < length - 1) {
      return children[i + 1].minPair(reusedArray);
    }
    return result;
  }
  checkValid(depth, tree, baseIndex) {
    let kL = this.keys.length, cL = this.children.length;
    check(kL === cL, "keys/children length mismatch: depth", depth, "lengths", kL, cL, "baseIndex", baseIndex);
    check(kL > 1 || depth > 0, "internal node has length", kL, "at depth", depth, "baseIndex", baseIndex);
    let size = 0, c = this.children, k = this.keys, childSize = 0;
    for (var i = 0;i < cL; i++) {
      size += c[i].checkValid(depth + 1, tree, baseIndex + size);
      childSize += c[i].keys.length;
      check(size >= childSize, "wtf", baseIndex);
      check(i === 0 || c[i - 1].constructor === c[i].constructor, "type mismatch, baseIndex:", baseIndex);
      if (c[i].maxKey() != k[i])
        check(false, "keys[", i, "] =", k[i], "is wrong, should be ", c[i].maxKey(), "at depth", depth, "baseIndex", baseIndex);
      if (!(i === 0 || tree._compare(k[i - 1], k[i]) < 0))
        check(false, "sort violation at depth", depth, "index", i, "keys", k[i - 1], k[i]);
    }
    let toofew = childSize === 0;
    if (toofew || childSize > tree.maxNodeSize * cL)
      check(false, toofew ? "too few" : "too many", "children (", childSize, size, ") at depth", depth, "maxNodeSize:", tree.maxNodeSize, "children.length:", cL, "baseIndex:", baseIndex);
    return size;
  }
  set(key, value, overwrite, tree) {
    var c = this.children, max = tree._maxNodeSize, cmp = tree._compare;
    var i = Math.min(this.indexOf(key, 0, cmp), c.length - 1), child = c[i];
    if (child.isShared)
      c[i] = child = child.clone();
    if (child.keys.length >= max) {
      var other;
      if (i > 0 && (other = c[i - 1]).keys.length < max && cmp(child.keys[0], key) < 0) {
        if (other.isShared)
          c[i - 1] = other = other.clone();
        other.takeFromRight(child);
        this.keys[i - 1] = other.maxKey();
      } else if ((other = c[i + 1]) !== undefined && other.keys.length < max && cmp(child.maxKey(), key) < 0) {
        if (other.isShared)
          c[i + 1] = other = other.clone();
        other.takeFromLeft(child);
        this.keys[i] = c[i].maxKey();
      }
    }
    var result = child.set(key, value, overwrite, tree);
    if (result === false)
      return false;
    this.keys[i] = child.maxKey();
    if (result === true)
      return true;
    if (this.keys.length < max) {
      this.insert(i + 1, result);
      return true;
    } else {
      var newRightSibling = this.splitOffRightSide(), target = this;
      if (cmp(result.maxKey(), this.maxKey()) > 0) {
        target = newRightSibling;
        i -= this.keys.length;
      }
      target.insert(i + 1, result);
      return newRightSibling;
    }
  }
  insert(i, child) {
    this.children.splice(i, 0, child);
    this.keys.splice(i, 0, child.maxKey());
  }
  splitOffRightSide() {
    var half = this.children.length >> 1;
    return new BNodeInternal(this.children.splice(half), this.keys.splice(half));
  }
  takeFromRight(rhs) {
    this.keys.push(rhs.keys.shift());
    this.children.push(rhs.children.shift());
  }
  takeFromLeft(lhs) {
    this.keys.unshift(lhs.keys.pop());
    this.children.unshift(lhs.children.pop());
  }
  forRange(low, high, includeHigh, editMode, tree, count, onFound) {
    var cmp = tree._compare;
    var keys = this.keys, children = this.children;
    var iLow = this.indexOf(low, 0, cmp), i = iLow;
    var iHigh = Math.min(high === low ? iLow : this.indexOf(high, 0, cmp), keys.length - 1);
    if (!editMode) {
      for (;i <= iHigh; i++) {
        var result = children[i].forRange(low, high, includeHigh, editMode, tree, count, onFound);
        if (typeof result !== "number")
          return result;
        count = result;
      }
    } else if (i <= iHigh) {
      try {
        for (;i <= iHigh; i++) {
          if (children[i].isShared)
            children[i] = children[i].clone();
          var result = children[i].forRange(low, high, includeHigh, editMode, tree, count, onFound);
          keys[i] = children[i].maxKey();
          if (typeof result !== "number")
            return result;
          count = result;
        }
      } finally {
        var half = tree._maxNodeSize >> 1;
        if (iLow > 0)
          iLow--;
        for (i = iHigh;i >= iLow; i--) {
          if (children[i].keys.length <= half) {
            if (children[i].keys.length !== 0) {
              this.tryMerge(i, tree._maxNodeSize);
            } else {
              keys.splice(i, 1);
              children.splice(i, 1);
            }
          }
        }
        if (children.length !== 0 && children[0].keys.length === 0)
          check(false, "emptiness bug");
      }
    }
    return count;
  }
  tryMerge(i, maxSize) {
    var children = this.children;
    if (i >= 0 && i + 1 < children.length) {
      if (children[i].keys.length + children[i + 1].keys.length <= maxSize) {
        if (children[i].isShared)
          children[i] = children[i].clone();
        children[i].mergeSibling(children[i + 1], maxSize);
        children.splice(i + 1, 1);
        this.keys.splice(i + 1, 1);
        this.keys[i] = children[i].maxKey();
        return true;
      }
    }
    return false;
  }
  mergeSibling(rhs, maxNodeSize) {
    var oldLength = this.keys.length;
    this.keys.push.apply(this.keys, rhs.keys);
    const rhsChildren = rhs.children;
    this.children.push.apply(this.children, rhsChildren);
    if (rhs.isShared && !this.isShared) {
      for (var i = 0;i < rhsChildren.length; i++)
        rhsChildren[i].isShared = true;
    }
    this.tryMerge(oldLength - 1, maxNodeSize);
  }
}
var undefVals = [];
var Delete = { delete: true };
var DeleteRange = () => Delete;
var Break = { break: true };
var EmptyLeaf = function() {
  var n = new BNode;
  n.isShared = true;
  return n;
}();
var EmptyArray = [];
var ReusedArray = [];
function check(fact, ...args) {
  if (!fact) {
    args.unshift("B+ tree");
    throw new Error(args.join(" "));
  }
}
var EmptyBTree = (() => {
  let t = new BTree;
  t.freeze();
  return t;
})();

// lsm-tree.ts
class LSM {
  memTable = new BTree;
  freezeTable = null;
  max_size;
  recoverFlush = BigInt(-1);
  constructor(max = 8) {
    this.max_size = max;
  }
  async recover(wal, sbm, er) {
    const scan = wal.scan(wal.getHead(), wal.getUsed());
    const requests = await scan;
    log(1 /* info */, "Starting recovery", { requests: requests.length, bytes: wal.getUsed() });
    const beforeRecov = wal.getLastLSN();
    this.recoverFlush = beforeRecov;
    requests.forEach((v) => {
      er.dispatch({
        op: OP_INV[v.op],
        key: v.key,
        value: v.value,
        next: null,
        ts: 0
      });
    });
  }
  async put(er, key, value) {
    this.memTable.set(key, value);
  }
  get(key) {
    const inMem = this.memTable.has(key);
    if (inMem)
      return this.memTable.get(key);
  }
  freeze() {
    this.freezeTable = this.memTable.clone();
    this.freezeTable.freeze();
  }
  needsFlush() {
    if (this.memTable.size >= this.max_size) {
      return true;
    }
  }
}

// intrusive-queue.ts
class IntrusiveQueue {
  _head = null;
  _tail = null;
  _len = 0;
  get length() {
    return this._len;
  }
  push(x) {
    x.next = null;
    if (this._tail) {
      this._tail.next = x;
      this._tail = x;
    } else {
      this._head = this._tail = x;
    }
    this._len++;
  }
  shift() {
    const x = this._head;
    if (!x)
      return null;
    this._head = x.next;
    if (this._head === null)
      this._tail = null;
    x.next = null;
    this._len--;
    return x;
  }
  takeUpTo(n) {
    const out = [];
    while (n-- > 0) {
      const x = this.shift();
      if (!x)
        break;
      out.push(x);
    }
    return out;
  }
}

// event-ring.ts
class EventRing {
  tree;
  walManager;
  tio;
  time;
  sbManager;
  q = new IntrusiveQueue;
  running = false;
  constructor(tree, walManager, tio, time, sbManager) {
    this.tree = tree;
    this.walManager = walManager;
    this.tio = tio;
    this.time = time;
    this.sbManager = sbManager;
  }
  start() {
    this.running = true;
  }
  dispatch(op) {
    op.ts = this.time.now;
    log(0 /* debug */, "Operation dispatched", { op: op.op, key: op.key });
    this.q.push(op);
  }
  async submit(tree, op) {
    return new Promise(async (r) => {
      switch (op.op) {
        case "check": {
          setImmediate(() => this.walManager.checkpoint(this.walManager.getLastLSN(), this.sbManager));
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
  async runFor(ms) {
    const ms_dur = BigInt(ms * 1e6);
    const end = this.time.now + ms_dur;
    while (true) {
      if (this.time.now >= end)
        break;
      const batch = this.q.takeUpTo(MAX_INFLIGHT);
      if (batch.length === 0) {
        continue;
      }
      if (this.tree.recoverFlush > 0n) {
        this.tree.recoverFlush = BigInt(-1);
      } else {
        await this.walManager.appendMany(batch);
      }
      if (this.sbManager) {
        await this.sbManager.checkpoint({
          checkpointLSN: this.walManager.getLastLSN(),
          jHead: BigInt(this.walManager.getHead()),
          jTail: BigInt(this.walManager.getTail())
        });
      }
      for (const op of batch) {
        await new Promise((r) => setImmediate(async () => {
          const v = await this.submit(this.tree, op);
          op.onComplete?.(v);
          r();
        }));
      }
      if (this.tree.needsFlush()) {
        this.tree.freeze();
        this.tree.memTable.clear();
        if (this.tree.freezeTable) {
          await this.tio.flushWAL(this.tree.freezeTable);
          await this.walManager.checkpoint(this.walManager.getLastLSN(), this.sbManager);
        }
      }
    }
  }
  async tick() {
    console.log("tick");
    if (!this.running)
      return;
    await this.runFor(10);
    setImmediate(() => this.tick());
  }
}

// manifest.ts
function encodeManifestEntry(me) {
  if (me.minPrefix.length !== PREFIX || me.maxPrefix.length !== PREFIX) {
    throw new DatabaseError(MANIFEST_RESULT.INVALID_PREFIX_SIZE, `prefixes must be exactly ${PREFIX} bytes`);
  }
  const buf = new Uint8Array(ENTRY_SIZE);
  const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  let o = 0;
  v.setUint16(o, me.level, true);
  o += 2;
  v.setUint16(o, 0, true);
  o += 2;
  v.setBigUint64(o, me.metaOff, true);
  o += 8;
  v.setUint32(o, me.metaLen, true);
  o += 4;
  buf.set(me.minPrefix, o);
  o += PREFIX;
  buf.set(me.maxPrefix, o);
  o += PREFIX;
  return buf;
}
function encodeManifestPage(mp) {
  if (mp.entries.length > CAP) {
    throw new DatabaseError(MANIFEST_RESULT.TOO_MANY_ENTRIES, `too many entries: ${mp.entries.length} > cap ${CAP}`);
  }
  const buf = new Uint8Array(BLOCK);
  const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  let o = 0;
  const version = mp.version ?? 1;
  v.setUint16(o, version, true);
  o += 2;
  v.setUint16(o, 0, true);
  o += 2;
  v.setBigUint64(o, mp.epoch, true);
  o += 8;
  v.setUint16(o, mp.entries.length, true);
  o += 2;
  v.setUint16(o, 0, true);
  o += 2;
  for (const e of mp.entries) {
    const data = encodeManifestEntry(e);
    buf.set(data, o);
    o += ENTRY_SIZE;
  }
  return buf;
}
function decodeManifestPage(buf) {
  if (buf.length !== BLOCK)
    throw new DatabaseError(MANIFEST_RESULT.INVALID_PAGE_SIZE, `manifest page must be ${BLOCK} bytes`);
  const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  let o = 0;
  const version = v.getUint16(o, true);
  o += 2;
  o += 2;
  const epoch = v.getBigUint64(o, true);
  o += 8;
  const count = v.getUint16(o, true);
  o += 2;
  o += 2;
  const allZero = version === 0 && epoch === 0n && count === 0;
  if (allZero)
    return { epoch: 0n, version: 0, entries: [] };
  if (count > CAP) {
    throw new DatabaseError(MANIFEST_RESULT.COUNT_EXCEEDS_CAP, `manifest count ${count} exceeds cap ${CAP}`);
  }
  const need = HEADER_SIZE + count * ENTRY_SIZE;
  if (need > buf.length) {
    throw new DatabaseError(MANIFEST_RESULT.CORRUPT, `manifest corrupt: need ${need}, have ${buf.length}`);
  }
  const entries = [];
  for (let i = 0;i < count; i++) {
    const eOff = HEADER_SIZE + i * ENTRY_SIZE;
    const ev = new DataView(buf.buffer, buf.byteOffset + eOff, ENTRY_SIZE);
    let eo = 0;
    const level = ev.getUint16(eo, true);
    eo += 2;
    eo += 2;
    const metaOff = ev.getBigUint64(eo, true);
    eo += 8;
    const metaLen = ev.getUint32(eo, true);
    eo += 4;
    const minPrefix = buf.subarray(eOff + eo, eOff + eo + PREFIX);
    eo += PREFIX;
    const maxPrefix = buf.subarray(eOff + eo, eOff + eo + PREFIX);
    eo += PREFIX;
    entries.push({
      level,
      metaOff,
      metaLen,
      minPrefix: new Uint8Array(minPrefix),
      maxPrefix: new Uint8Array(maxPrefix)
    });
  }
  return { epoch, version, entries };
}

// table.ts
function decodeIndex(buf) {
  const out = [];
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  let o = 0;
  while (o + 2 + 8 + 4 <= buf.length) {
    const klen = dv.getUint16(o, true);
    o += 2;
    const off = Number(dv.getBigUint64(o, true));
    o += 8;
    const len = dv.getUint32(o, true);
    o += 4;
    if (o + klen > buf.length)
      break;
    const key = buf.subarray(o, o + klen);
    o += klen;
    out.push({ firstKey: new Uint8Array(key), off, len });
  }
  return out;
}
function encodeTableMeta(meta) {
  const enc = new TextEncoder;
  if (meta.minKey.length !== PREFIX || meta.maxKey.length !== PREFIX) {
    throw new DatabaseError(TABLE_RESULT.INVALID_KEY_SIZE, `minKey/maxKey must be ${PREFIX} bytes`);
  }
  const idBytes = enc.encode(meta.id);
  const extCnt = meta.extents.length;
  const headerLen = 2 + 2 + 8 + 8 + 8 + 4 + 8 + 4 + 4 + PREFIX + PREFIX + 4;
  const extentsLen = extCnt * (8 + 4);
  const total = headerLen + idBytes.length + extentsLen;
  const buf = new Uint8Array(total);
  const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  let o = 0;
  v.setUint16(o, idBytes.length, true);
  o += 2;
  v.setUint16(o, meta.level, true);
  o += 2;
  v.setBigUint64(o, meta.seqMin, true);
  o += 8;
  v.setBigUint64(o, meta.seqMax, true);
  o += 8;
  v.setBigUint64(o, BigInt(meta.sizeBytes), true);
  o += 8;
  v.setUint32(o, meta.blockSize >>> 0, true);
  o += 4;
  v.setBigUint64(o, BigInt(meta.indexOff), true);
  o += 8;
  v.setUint32(o, meta.indexLen >>> 0, true);
  o += 4;
  v.setUint32(o, meta.entryCount >>> 0, true);
  o += 4;
  buf.set(meta.minKey, o);
  o += PREFIX;
  buf.set(meta.maxKey, o);
  o += PREFIX;
  v.setUint32(o, extCnt >>> 0, true);
  o += 4;
  buf.set(idBytes, o);
  o += idBytes.length;
  for (const e of meta.extents) {
    v.setBigUint64(o, BigInt(e.startBlock), true);
    o += 8;
    v.setUint32(o, e.blocks >>> 0, true);
    o += 4;
  }
  return buf;
}
function decodeTableMeta(buf) {
  const dec = new TextDecoder;
  const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  let o = 0;
  const idLen = v.getUint16(o, true);
  o += 2;
  const level = v.getUint16(o, true);
  o += 2;
  const seqMin = v.getBigUint64(o, true);
  o += 8;
  const seqMax = v.getBigUint64(o, true);
  o += 8;
  const sizeBytes = Number(v.getBigUint64(o, true));
  o += 8;
  const blockSize = v.getUint32(o, true);
  o += 4;
  const indexOff = Number(v.getBigUint64(o, true));
  o += 8;
  const indexLen = v.getUint32(o, true);
  o += 4;
  const entryCount = v.getUint32(o, true);
  o += 4;
  const minKey = buf.subarray(o, o + PREFIX);
  o += PREFIX;
  const maxKey = buf.subarray(o, o + PREFIX);
  o += PREFIX;
  const extCnt = v.getUint32(o, true);
  o += 4;
  if (o + idLen > buf.length) {
    throw new DatabaseError(TABLE_RESULT.TRUNCATED_ID, "decodeTableMeta: truncated id");
  }
  const id = dec.decode(buf.subarray(o, o + idLen));
  o += idLen;
  const extents = [];
  for (let i = 0;i < extCnt; i++) {
    if (o + 8 + 4 > buf.length)
      throw new DatabaseError(TABLE_RESULT.TRUNCATED_EXTENTS, "decodeTableMeta: truncated extents");
    const startBlock = Number(v.getBigUint64(o, true));
    o += 8;
    const blocks = v.getUint32(o, true);
    o += 4;
    extents.push({ startBlock, blocks });
  }
  return {
    id,
    level,
    minKey: new Uint8Array(minKey),
    maxKey: new Uint8Array(maxKey),
    seqMin,
    seqMax,
    extents,
    sizeBytes,
    blockSize,
    indexOff,
    indexLen,
    entryCount
  };
}

class TableIO {
  file;
  manifest = { epoch: 0n, entries: [] };
  tableTail = MANIFEST_OFF + BLOCK;
  map = new Map;
  constructor(file) {
    this.file = file;
  }
  initFrom(mp) {
    this.manifest = mp;
  }
  async formatInitial({ version = 1, epoch = 1n }) {
    const mp = {
      epoch,
      version,
      entries: []
    };
    const buf = encodeManifestPage(mp);
    await this.file.write(MANIFEST_OFF, buf);
    await this.file.fsync();
    this.manifest = mp;
  }
  get manifestEntryTail() {
    return MANIFEST_OFF + 16 + this.manifest.entries.length * 48;
  }
  async load() {
    const mp_buf = await this.file.read(MANIFEST_OFF, BLOCK);
    const mp = decodeManifestPage(mp_buf);
    this.manifest = mp;
    this.manifest.entries.forEach((e) => {
      this.tableTail += alignUp(e.metaLen, BLOCK);
    });
    return this;
  }
  async updateManifest() {
    const buf = encodeManifestPage(this.manifest);
    await this.file.write(MANIFEST_OFF, buf);
    await this.file.fsync();
  }
  async addEntry(e) {
    if (this.manifest.entries.length >= Math.floor((BLOCK - 16) / 48))
      throw new DatabaseError(TABLE_RESULT.MANIFEST_FULL, "Ran out of manifest entries");
    this.manifest.entries.push(e);
    this.updateManifest();
    await this.file.write(this.manifestEntryTail, encodeManifestEntry2(e));
    await this.file.fsync();
  }
  async requestTable(level, size, minPrefix, maxPrefix) {
    const fileBytes = await this.file.size();
    const left = fileBytes - this.tableTail;
    if (size > left)
      throw new DatabaseError(TABLE_RESULT.NEEDS_COMPACTION, "Cannot add another table. Needs compaction");
    const metaOff = this.tableTail;
    const metaLen = size;
    const e = {
      level,
      metaOff: BigInt(metaOff),
      metaLen,
      minPrefix,
      maxPrefix
    };
    await this.addEntry(e);
    this.tableTail = alignUp(metaOff + metaLen, BLOCK);
    return e;
  }
  concat(parts) {
    const total = parts.reduce((s, p) => s + p.length, 0);
    const out = new Uint8Array(total);
    let o = 0;
    for (const p of parts) {
      out.set(p, o);
      o += p.length;
    }
    return out;
  }
  async flushWAL(tree) {
    log(1 /* info */, "Starting table flush", { entryCount: tree.size });
    let enc = new TextEncoder;
    const blocks = [];
    let index = [];
    let countInBlock = 0;
    let parts = [];
    let curSize = 2;
    let firstKeyThisBlock = null;
    let minPrefix = null;
    let maxPrefix = null;
    let entryCount = 0;
    const pushRecord = (kb, vb) => {
      const recHdr = new Uint8Array(2 + 4);
      const dv = new DataView(recHdr.buffer);
      dv.setUint16(0, kb.length, true);
      dv.setUint32(2, vb.length, true);
      const recLen = recHdr.length + kb.length + vb.length;
      if (curSize + recLen > BLOCK)
        flushBlock();
      if (!firstKeyThisBlock)
        firstKeyThisBlock = kb;
      parts.push(recHdr, kb, vb);
      curSize += recLen;
      countInBlock++;
      entryCount++;
      const p = extractSortKey16(kb);
      if (!minPrefix || lt16(p, minPrefix))
        minPrefix = p;
      if (!maxPrefix || gt16(p, maxPrefix))
        maxPrefix = p;
    };
    const flushBlock = () => {
      if (countInBlock === 0)
        return;
      const hdr = new Uint8Array(2);
      new DataView(hdr.buffer).setUint16(0, countInBlock, true);
      const raw = this.concat([hdr, ...parts]);
      const padded = (() => {
        const need = alignUp(raw.length, BLOCK);
        if (need === raw.length)
          return raw;
        const out = new Uint8Array(need);
        out.set(raw, 0);
        return out;
      })();
      const off = blocks.reduce((s, b) => s + b.length, 0);
      blocks.push(padded);
      index.push({ firstKey: firstKeyThisBlock, off, len: padded.length });
      parts = [];
      curSize = 2;
      countInBlock = 0;
      firstKeyThisBlock = null;
    };
    tree.toArray().sort(([k, v], [k2, v2]) => {
      const k_b = enc.encode(k);
      const k2_b = enc.encode(k2);
      const k1_sorter = extractSortKey16(k_b);
      const k2_sorter = extractSortKey16(k2_b);
      return cmp16(k1_sorter, k2_sorter);
    }).forEach(([k, v]) => {
      const kb = enc.encode(k);
      const vb = enc.encode(v);
      pushRecord(kb, vb);
    });
    flushBlock();
    if (blocks.length === 0)
      return;
    const indexParts = [];
    for (const e of index) {
      const hdr = new Uint8Array(2 + 8 + 4);
      const dv = new DataView(hdr.buffer);
      dv.setUint16(0, e.firstKey.length, true);
      dv.setBigUint64(2, BigInt(e.off), true);
      dv.setUint32(10, e.len, true);
      indexParts.push(hdr, e.firstKey);
    }
    const indexRaw = this.concat(indexParts);
    const indexBuf = (() => {
      const need = alignUp(indexRaw.length, 8);
      const out = new Uint8Array(need);
      out.set(indexRaw, 0);
      return out;
    })();
    const indexLen = indexRaw.length;
    const indexLenPadded = indexBuf.length;
    const blockBytes = blocks.reduce((s, b) => s + b.length, 0);
    const sizeBytes = BLOCK + indexLenPadded + blockBytes;
    const entry = await this.requestTable(0, sizeBytes, minPrefix ?? new Uint8Array(16), maxPrefix ?? new Uint8Array(16));
    const metaOff = Number(entry.metaOff);
    const indexOff = metaOff + BLOCK;
    const meta = {
      id: crypto.randomUUID(),
      level: 0,
      minKey: minPrefix ?? new Uint8Array(16),
      maxKey: maxPrefix ?? new Uint8Array(16),
      seqMin: 0n,
      seqMax: 0n,
      extents: [],
      sizeBytes,
      blockSize: BLOCK,
      indexOff,
      indexLen,
      entryCount
    };
    const encoded = encodeTableMeta(meta);
    const alignedMetaBlock = new Uint8Array(BLOCK);
    alignedMetaBlock.set(encoded, 0);
    const full = this.concat([alignedMetaBlock, indexBuf, ...blocks]);
    if (full.byteLength !== entry.metaLen) {
      throw new DatabaseError(TABLE_RESULT.BROKEN_TABLE_SIZE, `broken table size: ${full.byteLength} !== ${entry.metaLen}`);
    }
    await this.file.write(metaOff, full);
    await this.file.fsync();
    log(1 /* info */, "Table flushed", { id: meta.id, sizeBytes, entryCount });
  }
  async readEntryHead(i) {
    const e = this.manifest.entries[i];
    if (!e)
      throw new DatabaseError(TABLE_RESULT.ENTRY_NOT_EXIST, "entry doesn't exist");
    if (this.map.has(e.metaOff)) {
      log(0 /* debug */, "Head cache hit", { saved: e.metaLen });
      return this.map.get(e.metaOff);
    }
    const metaOff = Number(e.metaOff);
    const metaBuf = await this.file.read(metaOff, BLOCK);
    const table = decodeTableMeta(metaBuf);
    const indexRaw = await this.file.read(table.indexOff, table.indexLen);
    const idxRel = decodeIndex(indexRaw);
    const dataStart = table.indexOff + alignUp(table.indexLen, 8);
    const indexAbs = idxRel.map((ent) => ({
      firstKey: ent.firstKey,
      off: dataStart + ent.off,
      len: ent.len
    }));
    const res = { index: indexAbs, table };
    this.map.set(e.metaOff, res);
    return res;
  }
  async aggHeads(level = 0) {
    return (await Promise.all(this.manifest.entries.map(async (_, i) => {
      const head = await this.readEntryHead(i);
      if (head.table.level === level)
        return head;
    }))).filter((v) => v !== undefined);
  }
  async levelSize(level) {
    let count = 0;
    for (const head of await this.aggHeads(level)) {
      count += head.table.entryCount;
    }
    return count;
  }
}

class TableReader {
  file;
  meta;
  block = new Uint8Array(0);
  prevKey = new Uint8Array(0);
  count = 0;
  localPos = 0;
  iter = 0;
  i = 0;
  constructor(file, meta) {
    this.file = file;
    this.meta = meta;
  }
  async loadBlock(e) {
    this.block = await this.file.read(e.off, e.len);
    const dv = new DataView(this.block.buffer, this.block.byteOffset);
    this.count = dv.getUint16(0, true);
    this.localPos = 2;
    this.iter = 0;
    this.prevKey = new Uint8Array(0);
  }
  async next() {
    while (true) {
      if (this.block.length === 0) {
        if (this.i >= this.meta.index.length)
          return null;
        await this.loadBlock(this.meta.index[this.i++]);
      }
      if (this.iter >= this.count) {
        this.block = new Uint8Array(0);
        continue;
      }
      const dv = new DataView(this.block.buffer, this.block.byteOffset + this.localPos);
      const klen = dv.getUint16(0, true);
      const vlen = dv.getUint32(2, true);
      this.localPos += 2 + 4;
      const key = this.block.subarray(this.localPos, this.localPos + klen);
      this.localPos += klen;
      const value = this.block.subarray(this.localPos, this.localPos + vlen);
      this.localPos += vlen;
      this.iter++;
      return { key, value };
    }
  }
}
function encodeManifestEntry2(me) {
  if (me.minPrefix.length !== PREFIX || me.maxPrefix.length !== PREFIX) {
    throw new DatabaseError(TABLE_RESULT.INVALID_PREFIX_SIZE, `prefixes must be exactly ${PREFIX} bytes`);
  }
  const buf = new Uint8Array(ENTRY_SIZE);
  const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  let o = 0;
  v.setUint16(o, me.level, true);
  o += 2;
  v.setUint16(o, 0, true);
  o += 2;
  v.setBigUint64(o, me.metaOff, true);
  o += 8;
  v.setUint32(o, me.metaLen, true);
  o += 4;
  buf.set(me.minPrefix, o);
  o += PREFIX;
  buf.set(me.maxPrefix, o);
  o += PREFIX;
  return buf;
}

// clock.ts
class Clock {
  _epoch = 0n;
  current = () => process.hrtime.bigint();
  lastCurrent = 0n;
  constructor(system) {
    if (system)
      this.current = system;
    this.epoch = this.now;
  }
  get epoch() {
    return this._epoch;
  }
  set epoch(t) {
    if (t > this.now) {
      throw new DatabaseError(CLOCK_RESULT.CORRUPTED_EPOCH, "Persisted epoch is corrupted");
    }
    this._epoch = t + (this.now - t);
  }
  loadEpoc(sbm) {
    if (!sbm.current())
      throw new DatabaseError(SUPERBLOCK_RESULT.NO_VALID_SUPERBLOCKS, "No presisted epoch, super block is corrupt");
    this.epoch = sbm.current().epoch;
  }
  get now() {
    let newest = this.current();
    let retries = 0;
    while (this.lastCurrent >= newest) {
      this.lastCurrent = newest;
      const delay = Math.min(Math.pow(2, retries) * 1e6, 1e9);
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, delay / 1e6);
      newest = this.current();
      retries++;
      if (retries === 8)
        throw new DatabaseError(CLOCK_RESULT.BROKEN_CLOCK_STATE, "Broken clock state");
    }
    this.lastCurrent = newest;
    return newest;
  }
  get since() {
    return this.now - this.epoch;
  }
}

// index.ts
var path = "wal.bin";
var io = new FileIO(path);
try {
  await io.open("r+");
} catch {
  await io.open("w+");
}
var wal = new WAL_Manager(io);
var sbm = new SuperblockManager(io);
var time = new Clock;
var tio = new TableIO(io);
var fileSize = await io.size();
if (fileSize === 0) {
  await sbm.formatInitial({
    journalStart: J_START,
    epoch: BigInt(Date.now())
  });
  wal.format(1073741824);
  await tio.formatInitial({ epoch: BigInt(Date.now()) });
} else {
  const sb = await sbm.load();
  time.loadEpoc(sbm);
  tio = await tio.load();
  await wal.initFrom(Number(sb.jHead), Number(sb.jTail), sb.checkpointLSN);
}
var t = new LSM(8);
var er = new EventRing(t, wal, tio, time, sbm);
if (wal.getUsed() > 0) {
  await t.recover(wal, sbm, er);
}
function fill(a) {
  for (let i = 0;i < a; i++) {
    er.dispatch({
      op: "set",
      key: `${i}`,
      value: "hi",
      ts: 0n,
      next: null
    });
  }
}
fill(9);
process.on("SIGINT", () => {
  console.log("hi");
  process.exit();
});
er.start();
er.tick();
var readers = [];
for (const head of await tio.aggHeads(0)) {
  const tr = new TableReader(io, head);
  readers.push(tr);
}
