package io.cascadestore.lsm.benchmark.ycsb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.Status;

class LevelDbShardedYcsbClientTest {

  @TempDir java.nio.file.Path tempDir;

  @Test
  void putGetRoundTrip() throws Exception {
    LevelDbShardedYcsbClient client = new LevelDbShardedYcsbClient();
    Properties props = new Properties();
    props.setProperty(LevelDbYcsbFactory.PROP_DATADIR, tempDir.resolve("leveldb").toString());
    props.setProperty(LevelDbYcsbFactory.PROP_RESET_DATADIR, "true");
    props.setProperty(LevelDbYcsbFactory.PROP_MEMTABLE_MB, "4");
    props.setProperty(LevelDbYcsbFactory.PROP_BLOCK_CACHE_MB, "8");
    props.setProperty(LevelDbYcsbFactory.PROP_SHARDS, "1");
    client.setProperties(props);
    client.init();

    try {
      Map<String, site.ycsb.ByteIterator> values = new HashMap<>();
      values.put("field0", new ByteArrayByteIterator("hello".getBytes()));
      assertEquals(Status.OK, client.insert("usertable", "user1", values));

      Set<String> fields = new HashSet<>();
      fields.add("field0");
      Map<String, site.ycsb.ByteIterator> result = new HashMap<>();
      assertEquals(Status.OK, client.read("usertable", "user1", fields, result));
      assertNotNull(result.get("field0"));
      assertEquals("hello", new String(result.get("field0").toArray()));
    } finally {
      client.cleanup();
    }
  }
}
