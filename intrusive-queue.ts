export interface Link<T> {
    next: T | null;
}

export class IntrusiveQueue<T extends Link<T>> {
    private _head: T | null = null;
    private _tail: T | null = null;
    private _len = 0;

    get length(): number {
        return this._len;
    }

    push(x: T): void {
        x.next = null;
        if (this._tail) {
            this._tail.next = x;
            this._tail = x;
        } else {
            this._head = this._tail = x;
        }
        this._len++;
    }

    shift(): T | null {
        const x = this._head;
        if (!x) return null;
        this._head = x.next;
        if (this._head === null) this._tail = null;
        x.next = null;
        this._len--;
        return x;
    }

    takeUpTo(n: number): T[] {
        const out: T[] = [];
        while (n-- > 0) {
            const x = this.shift();
            if (!x) break;
            out.push(x);
        }
        return out;
    }
}