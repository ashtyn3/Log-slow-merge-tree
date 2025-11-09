export type Op = "set" | "del" | "get" | "check";
export const OP = { set: 1, del: 2, get: 3, check: 4 } as const;
export const OP_INV: Record<number, Op> = { 1: "set", 2: "del", 3: "get", 4: "check" };

export const MAX_INFLIGHT = 8;
