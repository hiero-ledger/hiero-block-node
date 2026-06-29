export function asBuffer(value: unknown): Buffer {
  if (value instanceof Uint8Array) {
    return Buffer.from(value);
  }
  if (Buffer.isBuffer(value)) {
    return value;
  }
  throw new Error("Expected bytes-like value");
}

export function bytesToHex(value: unknown): string {
  return asBuffer(value).toString("hex");
}

export function longToString(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number") {
    return String(value);
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (value && typeof value === "object" && "toString" in value) {
    return String(value.toString());
  }
  throw new Error("Unable to coerce long-like value to string");
}
