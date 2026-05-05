package io.cascadestore.lsm.benchmark.ycsb;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;

/** Encodes YCSB field maps into a single value blob for CascadeStore keys. */
final class YcsbRecordCodec {

  private YcsbRecordCodec() {}

  static byte[] tablePrefix(String table) {
    return (table + ":").getBytes(StandardCharsets.UTF_8);
  }

  static byte[] storageKey(byte[] tablePrefix, String userKey) {
    byte[] userBytes = userKey.getBytes(StandardCharsets.UTF_8);
    byte[] key = new byte[tablePrefix.length + userBytes.length];
    System.arraycopy(tablePrefix, 0, key, 0, tablePrefix.length);
    System.arraycopy(userBytes, 0, key, tablePrefix.length, userBytes.length);
    return key;
  }

  static byte[] storageKey(String table, String userKey) {
    return storageKey(tablePrefix(table), userKey);
  }

  static byte[] scanEndKey(String table) {
    return (table + ";").getBytes(StandardCharsets.UTF_8);
  }

  static byte[] encode(Map<String, ByteIterator> values) {
    int fieldCount = values.size();
    int size = Integer.BYTES;
    Map<String, byte[]> fieldBytes = new HashMap<>();

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      byte[] name = entry.getKey().getBytes(StandardCharsets.UTF_8);
      byte[] value = entry.getValue().toArray();
      fieldBytes.put(entry.getKey(), value);
      size += Short.BYTES + name.length + Integer.BYTES + value.length;
    }

    ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.putInt(fieldCount);
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      byte[] name = entry.getKey().getBytes(StandardCharsets.UTF_8);
      byte[] value = fieldBytes.get(entry.getKey());
      buffer.putShort((short) name.length);
      buffer.put(name);
      buffer.putInt(value.length);
      buffer.put(value);
    }
    return buffer.array();
  }

  static void decodeInto(
      byte[] blob, Set<String> requestedFields, Map<String, ByteIterator> result) {
    if (blob == null || blob.length == 0) {
      return;
    }

    ByteBuffer buffer = ByteBuffer.wrap(blob);
    int fieldCount = buffer.getInt();
    boolean readAll = requestedFields == null || requestedFields.isEmpty();

    for (int i = 0; i < fieldCount; i++) {
      int nameLen = Short.toUnsignedInt(buffer.getShort());
      byte[] nameBytes = new byte[nameLen];
      buffer.get(nameBytes);
      String fieldName = new String(nameBytes, StandardCharsets.UTF_8);

      int valueLen = buffer.getInt();
      byte[] valueBytes = new byte[valueLen];
      buffer.get(valueBytes);

      if (readAll || requestedFields.contains(fieldName)) {
        result.put(fieldName, new ByteArrayByteIterator(valueBytes));
      }
    }
  }

  static byte[] merge(byte[] existing, Map<String, ByteIterator> updates) {
    if (updates.size() == 1) {
      Map.Entry<String, ByteIterator> entry = updates.entrySet().iterator().next();
      byte[] fieldName = entry.getKey().getBytes(StandardCharsets.UTF_8);
      byte[] newValue = entry.getValue().toArray();
      byte[] patched = patchSingleField(existing, fieldName, newValue);
      if (patched != null) {
        return patched;
      }
    }

    Map<String, ByteIterator> merged = new HashMap<>();
    decodeInto(existing, null, merged);
    for (Map.Entry<String, ByteIterator> entry : updates.entrySet()) {
      merged.put(entry.getKey(), entry.getValue());
    }
    return encode(merged);
  }

  /**
   * Patches one field in an encoded record without full decode/re-encode when possible.
   *
   * @return patched bytes, or {@code null} to fall back to full merge
   */
  static byte[] patchSingleField(byte[] existing, byte[] fieldName, byte[] newValue) {
    if (existing == null || existing.length < Integer.BYTES) {
      return null;
    }

    ByteBuffer buffer = ByteBuffer.wrap(existing);
    int fieldCount = buffer.getInt();
    int offset = buffer.position();

    for (int i = 0; i < fieldCount; i++) {
      if (offset + Short.BYTES > existing.length) {
        return null;
      }
      int nameLen = Short.toUnsignedInt(readShortAt(existing, offset));
      offset += Short.BYTES;
      if (offset + nameLen + Integer.BYTES > existing.length) {
        return null;
      }
      int valueLen = readIntAt(existing, offset + nameLen);
      int valueStart = offset + nameLen + Integer.BYTES;
      int fieldEnd = valueStart + valueLen;
      if (fieldEnd > existing.length) {
        return null;
      }

      if (nameLen == fieldName.length
          && rangeEquals(existing, offset, fieldName, 0, nameLen)) {
        int delta = newValue.length - valueLen;
        byte[] patched = new byte[existing.length + delta];
        System.arraycopy(existing, 0, patched, 0, valueStart);
        if (newValue.length != valueLen) {
          writeIntAt(patched, offset + nameLen, newValue.length);
        }
        System.arraycopy(newValue, 0, patched, valueStart, newValue.length);
        System.arraycopy(
            existing, fieldEnd, patched, valueStart + newValue.length, existing.length - fieldEnd);
        return patched;
      }

      offset = fieldEnd;
    }

    // Append a new field when the update names a field not present in the blob.
    int appendSize =
        Short.BYTES + fieldName.length + Integer.BYTES + newValue.length;
    byte[] patched = new byte[existing.length + appendSize];
    writeIntAt(patched, 0, fieldCount + 1);
    System.arraycopy(existing, Integer.BYTES, patched, Integer.BYTES, existing.length - Integer.BYTES);
    int appendOffset = existing.length;
    writeShortAt(patched, appendOffset, (short) fieldName.length);
    appendOffset += Short.BYTES;
    System.arraycopy(fieldName, 0, patched, appendOffset, fieldName.length);
    appendOffset += fieldName.length;
    writeIntAt(patched, appendOffset, newValue.length);
    appendOffset += Integer.BYTES;
    System.arraycopy(newValue, 0, patched, appendOffset, newValue.length);
    return patched;
  }

  private static short readShortAt(byte[] data, int offset) {
    return (short) ((data[offset] << 8) | (data[offset + 1] & 0xFF));
  }

  private static int readIntAt(byte[] data, int offset) {
    return ((data[offset] & 0xFF) << 24)
        | ((data[offset + 1] & 0xFF) << 16)
        | ((data[offset + 2] & 0xFF) << 8)
        | (data[offset + 3] & 0xFF);
  }

  private static void writeShortAt(byte[] data, int offset, short value) {
    data[offset] = (byte) (value >> 8);
    data[offset + 1] = (byte) value;
  }

  private static void writeIntAt(byte[] data, int offset, int value) {
    data[offset] = (byte) (value >> 24);
    data[offset + 1] = (byte) (value >> 16);
    data[offset + 2] = (byte) (value >> 8);
    data[offset + 3] = (byte) value;
  }

  private static boolean rangeEquals(
      byte[] left, int leftOffset, byte[] right, int rightOffset, int length) {
    if (leftOffset < 0 || rightOffset < 0 || length < 0) {
      return false;
    }
    if (leftOffset + length > left.length || rightOffset + length > right.length) {
      return false;
    }
    for (int i = 0; i < length; i++) {
      if (left[leftOffset + i] != right[rightOffset + i]) {
        return false;
      }
    }
    return true;
  }
}
