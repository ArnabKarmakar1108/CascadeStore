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

  static byte[] storageKey(String table, String userKey) {
    return (table + ":" + userKey).getBytes(StandardCharsets.UTF_8);
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
    Map<String, ByteIterator> merged = new HashMap<>();
    decodeInto(existing, null, merged);
    for (Map.Entry<String, ByteIterator> entry : updates.entrySet()) {
      merged.put(entry.getKey(), entry.getValue());
    }
    return encode(merged);
  }
}
