/**
 * Converter utilities for protobuf Any type conversion
 */

const TYPE_URL_PREFIX = 'type.googleapis.com/google.protobuf.';

export interface AnyValue {
  type_url: string;
  value: Buffer;
}

/**
 * Convert a JavaScript value to protobuf Any format
 */
export function toAny(value: unknown): AnyValue {
  if (value === null || value === undefined) {
    return {
      type_url: `${TYPE_URL_PREFIX}Empty`,
      value: Buffer.alloc(0),
    };
  }

  if (typeof value === 'string') {
    // StringValue: field 1 is the string
    const strBytes = Buffer.from(value, 'utf8');
    const lenBytes = encodeVarint(strBytes.length);
    const buf = Buffer.alloc(1 + lenBytes.length + strBytes.length);
    buf[0] = 0x0a; // field 1, wire type 2 (length-delimited)
    lenBytes.copy(buf, 1);
    strBytes.copy(buf, 1 + lenBytes.length);
    return {
      type_url: `${TYPE_URL_PREFIX}StringValue`,
      value: buf,
    };
  }

  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      // Int64Value: field 1 is int64
      const buf = Buffer.alloc(9);
      buf[0] = 0x08; // field 1, wire type 0 (varint)
      const len = writeVarint64(buf, 1, BigInt(value));
      return {
        type_url: `${TYPE_URL_PREFIX}Int64Value`,
        value: buf.subarray(0, len),
      };
    } else {
      // DoubleValue: field 1 is double
      const buf = Buffer.alloc(9);
      buf[0] = 0x09; // field 1, wire type 1 (64-bit)
      buf.writeDoubleLE(value, 1);
      return {
        type_url: `${TYPE_URL_PREFIX}DoubleValue`,
        value: buf,
      };
    }
  }

  if (typeof value === 'bigint') {
    const buf = Buffer.alloc(11);
    buf[0] = 0x08; // field 1, wire type 0 (varint)
    const len = writeVarint64(buf, 1, value);
    return {
      type_url: `${TYPE_URL_PREFIX}Int64Value`,
      value: buf.subarray(0, len),
    };
  }

  if (typeof value === 'boolean') {
    return {
      type_url: `${TYPE_URL_PREFIX}BoolValue`,
      value: Buffer.from([0x08, value ? 0x01 : 0x00]),
    };
  }

  if (value instanceof Date) {
    const seconds = BigInt(Math.floor(value.getTime() / 1000));
    const nanos = (value.getTime() % 1000) * 1000000;
    const buf = Buffer.alloc(20);
    let offset = 0;
    buf[offset++] = 0x08; // field 1, wire type 0
    offset = writeVarint64(buf, offset, seconds);
    buf[offset++] = 0x10; // field 2, wire type 0
    offset = writeVarint32(buf, offset, nanos);
    return {
      type_url: `${TYPE_URL_PREFIX}Timestamp`,
      value: buf.subarray(0, offset),
    };
  }

  if (Buffer.isBuffer(value)) {
    const lenBytes = encodeVarint(value.length);
    const buf = Buffer.alloc(1 + lenBytes.length + value.length);
    buf[0] = 0x0a; // field 1, wire type 2
    lenBytes.copy(buf, 1);
    value.copy(buf, 1 + lenBytes.length);
    return {
      type_url: `${TYPE_URL_PREFIX}BytesValue`,
      value: buf,
    };
  }

  throw new Error(`Unsupported type: ${typeof value}`);
}

/**
 * Convert a protobuf Any value to JavaScript value
 */
export function fromAny(any: AnyValue): unknown {
  if (!any || !any.type_url) {
    return null;
  }

  const typeUrl = any.type_url;
  const value = any.value;

  switch (typeUrl) {
    case `${TYPE_URL_PREFIX}Empty`:
      return null;

    case `${TYPE_URL_PREFIX}StringValue`:
      return parseStringValue(value);

    case `${TYPE_URL_PREFIX}DoubleValue`:
      return parseDoubleValue(value);

    case `${TYPE_URL_PREFIX}FloatValue`:
      return parseFloatValue(value);

    case `${TYPE_URL_PREFIX}Int64Value`:
    case `${TYPE_URL_PREFIX}UInt64Value`:
      return parseVarintValue(value);

    case `${TYPE_URL_PREFIX}Int32Value`:
    case `${TYPE_URL_PREFIX}UInt32Value`:
      return parseVarint32Value(value);

    case `${TYPE_URL_PREFIX}BoolValue`:
      return parseBoolValue(value);

    case `${TYPE_URL_PREFIX}Timestamp`:
      return parseTimestampValue(value);

    case `${TYPE_URL_PREFIX}BytesValue`:
      return parseBytesValue(value);

    default:
      throw new Error(`Unsupported type: ${typeUrl}`);
  }
}

function parseStringValue(buf: Buffer): string {
  if (buf.length < 2) return '';
  // Skip field tag (0x0a), read length, then string
  let offset = 1;
  const { value: len, bytesRead } = decodeVarint(buf, offset);
  offset += bytesRead;
  return buf.toString('utf8', offset, offset + Number(len));
}

function parseDoubleValue(buf: Buffer): number {
  if (buf.length < 9) return 0;
  return buf.readDoubleLE(1);
}

function parseFloatValue(buf: Buffer): number {
  if (buf.length < 5) return 0;
  return buf.readFloatLE(1);
}

function parseVarintValue(buf: Buffer): number {
  if (buf.length < 2) return 0;
  const { value } = decodeVarint(buf, 1);
  return Number(value);
}

function parseVarint32Value(buf: Buffer): number {
  if (buf.length < 2) return 0;
  const { value } = decodeVarint(buf, 1);
  return Number(value);
}

function parseBoolValue(buf: Buffer): boolean {
  if (buf.length < 2) return false;
  return buf[1] !== 0;
}

function parseTimestampValue(buf: Buffer): Date {
  let offset = 1;
  const { value: seconds, bytesRead } = decodeVarint(buf, offset);
  offset += bytesRead;
  let nanos = 0;
  if (offset < buf.length && buf[offset] === 0x10) {
    offset++;
    const result = decodeVarint(buf, offset);
    nanos = Number(result.value);
  }
  return new Date(Number(seconds) * 1000 + Math.floor(nanos / 1000000));
}

function parseBytesValue(buf: Buffer): Buffer {
  if (buf.length < 2) return Buffer.alloc(0);
  let offset = 1;
  const { value: len, bytesRead } = decodeVarint(buf, offset);
  offset += bytesRead;
  return buf.subarray(offset, offset + Number(len));
}

function encodeVarint(value: number): Buffer {
  const bytes: number[] = [];
  let v = value;
  while (v > 0x7f) {
    bytes.push((v & 0x7f) | 0x80);
    v >>>= 7;
  }
  bytes.push(v);
  return Buffer.from(bytes);
}

function writeVarint32(buf: Buffer, offset: number, value: number): number {
  let v = value;
  while (v > 0x7f) {
    buf[offset++] = (v & 0x7f) | 0x80;
    v >>>= 7;
  }
  buf[offset++] = v;
  return offset;
}

function writeVarint64(buf: Buffer, offset: number, value: bigint): number {
  let v = value < 0n ? BigInt.asUintN(64, value) : value;
  while (v > 0x7fn) {
    buf[offset++] = Number(v & 0x7fn) | 0x80;
    v >>= 7n;
  }
  buf[offset++] = Number(v);
  return offset;
}

function decodeVarint(buf: Buffer, offset: number): { value: bigint; bytesRead: number } {
  let result = 0n;
  let shift = 0n;
  let bytesRead = 0;

  while (offset + bytesRead < buf.length) {
    const byte = buf[offset + bytesRead];
    result |= BigInt(byte & 0x7f) << shift;
    bytesRead++;
    if ((byte & 0x80) === 0) {
      break;
    }
    shift += 7n;
  }

  return { value: result, bytesRead };
}
