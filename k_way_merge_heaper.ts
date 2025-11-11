import type { TableReader } from "./table";
import { cmp16, extractSortKey16 } from "./utils";

type Cur = { key: Uint8Array; value: Uint8Array; seq?: bigint; tombstone?: boolean };
type Src = { id: number; cur: Cur; next: () => Promise<Cur | null>, p16: Uint8Array };

function cmpBytes(a: Uint8Array, b: Uint8Array): number {
    const n = Math.min(a.length, b.length);
    for (let i = 0; i < n; i++) {
        const d = a[i] - b[i];
        if (d !== 0) return d;
    }
    return a.length - b.length;
}

class Heap<T> {
    constructor(private a: T[], private cmp: (x: T, y: T) => number) {
        this.heapify();
    }

    get size(): number {
        return this.a.length;
    }

    push(x: T): void {
        this.a.push(x);
        this.up(this.a.length - 1);
    }

    pop(): T | undefined {
        const n = this.a.length;
        if (n === 0) return undefined;

        const top = this.a[0];
        const last = this.a.pop()!;
        if (n > 1) {
            this.a[0] = last;
            this.down(0);
        }
        return top;
    }

    // Internal helpers ---------------------------------------------------------

    private heapify(): void {
        for (let i = (this.a.length >> 1) - 1; i >= 0; i--) {
            this.down(i);
        }
    }

    private swap(i: number, j: number): void {
        [this.a[i], this.a[j]] = [this.a[j]!, this.a[i]!];
    }

    private up(i: number): void {
        while (i > 0) {
            const p = (i - 1) >> 1;
            // If parent <= child, heap property holds (min-heap)
            if (this.cmp(this.a[p]!, this.a[i]!) <= 0) break;
            this.swap(p, i);
            i = p;
        }
    }

    private down(i: number): void {
        const n = this.a.length;
        for (; ;) {
            const l = (i << 1) + 1;
            if (l >= n) break; // no children

            const r = l + 1;
            // Pick the smaller child (min-heap)
            let m = l;
            if (r < n && this.cmp(this.a[r]!, this.a[l]!) < 0) m = r;

            if (this.cmp(this.a[i]!, this.a[m]!) <= 0) break;

            this.swap(i, m);
            i = m;
        }
    }
}

function keyNum16(p16: Uint8Array): number {
    let x = 0;
    for (let i = 0; i < 8; i++) x = (x << 8) | p16[i];
    return x >>> 0;
}

export async function* kWayMerger(readers: TableReader[]) {
    // prime heap
    const list: Src[] = [];
    for (let i = 0; i < readers.length; i++) {
        const cur = await readers[i]!.next();
        if (cur) list.push({ id: i, cur, p16: extractSortKey16(cur.key), next: () => readers[i]!.next() });
    }
    const heap = new Heap<Src>(list, (s) => keyNum16(s.p16));

    while (heap.size > 0) {
        const first = heap.pop()!;
        const bucket: Src[] = [first];

        // gather same 16-byte prefix
        for (; ;) {
            const n = heap.pop();
            if (!n) break;
            if (cmp16(n.p16, first.p16) === 0) bucket.push(n);
            else { heap.push(n); break; }
        }

        // Within prefix bucket, merge by full key and emit ALL keys:
        // Build a min-heap by full key on heads from each source in the bucket
        const byKey = new Heap<Src>(bucket, (s) => {
            // cheap numeric for heap; actual equality/order uses cmpBytes at extraction time
            // we can map first 8 bytes of full key
            let x = 0, k = s.cur.key, n = Math.min(8, k.length);
            for (let i = 0; i < n; i++) x = (x << 8) | k[i]!;
            return x >>> 0;
        });

        // pull until the prefix bucket is exhausted
        while (byKey.size > 0) {
            const a = byKey.pop()!;
            // collect all with exact same full key
            const same: Src[] = [a];
            for (; ;) {
                const b = byKey.pop();
                if (!b) break;
                if (cmpBytes(b.cur.key, a.cur.key) === 0) same.push(b);
                else { byKey.push(b); break; }
            }

            // choose newest (or last) among same-key entries; drop tombstones
            let best: Src | null = null;
            for (const s of same) {
                if (!best) best = s;
                // if you track seq: if ((s.cur.seq ?? 0n) > (best.cur.seq ?? 0n)) best = s;
                else best = s; // LWW fallback
            }
            if (best && !best.cur.tombstone && best.cur.value.length > 0) {
                yield best.cur;
            }

            // advance all sources from 'same' and put back if they still share prefix
            for (const s of same) {
                const nxt = await s.next();
                if (nxt) {
                    s.cur = nxt;
                    s.p16 = extractSortKey16(nxt.key);
                    if (cmp16(s.p16, first.p16) === 0) {
                        byKey.push(s); // still same prefix group
                    } else {
                        heap.push(s);  // moved to a new prefix; return to outer heap
                    }
                }
            }
        }
    }
}
