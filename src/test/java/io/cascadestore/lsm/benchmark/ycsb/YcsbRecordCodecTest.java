package io.cascadestore.lsm.benchmark.ycsb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import org.junit.jupiter.api.Test;

class YcsbRecordCodecTest {

  @Test
  void patchSingleFieldReplacesExistingValue() {
    Map<String, ByteIterator> initial = new HashMap<>();
    initial.put("field0", new ByteArrayByteIterator("alpha".getBytes()));
    initial.put("field1", new ByteArrayByteIterator("beta".getBytes()));
    byte[] existing = YcsbRecordCodec.encode(initial);

    Map<String, ByteIterator> updates =
        Map.of("field1", new ByteArrayByteIterator("gamma".getBytes()));
    byte[] merged = YcsbRecordCodec.merge(existing, updates);

    Map<String, ByteIterator> decoded = new HashMap<>();
    YcsbRecordCodec.decodeInto(merged, null, decoded);
    assertEquals("alpha", decoded.get("field0").toString());
    assertEquals("gamma", decoded.get("field1").toString());
  }

  @Test
  void patchSingleFieldMatchesFullMerge() {
    Map<String, ByteIterator> initial = new HashMap<>();
    initial.put("field0", new ByteArrayByteIterator("one".getBytes()));
    initial.put("field1", new ByteArrayByteIterator("two".getBytes()));
    byte[] existing = YcsbRecordCodec.encode(initial);

    Map<String, ByteIterator> updates =
        Map.of("field0", new ByteArrayByteIterator("updated".getBytes(StandardCharsets.UTF_8)));
    byte[] fast = YcsbRecordCodec.merge(existing, updates);

    Map<String, ByteIterator> mergedMap = new HashMap<>();
    YcsbRecordCodec.decodeInto(existing, null, mergedMap);
    mergedMap.put("field0", updates.get("field0"));
    byte[] slow = YcsbRecordCodec.encode(mergedMap);

    assertArrayEquals(slow, fast);
  }

  @Test
  void storageKeyUsesTablePrefix() {
    byte[] prefix = YcsbRecordCodec.tablePrefix("usertable");
    byte[] key = YcsbRecordCodec.storageKey(prefix, "user1");
    assertEquals("usertable:user1", new String(key, StandardCharsets.UTF_8));
  }
}
