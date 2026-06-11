package io.cascadestore.lsm.ycsb;

import static org.junit.jupiter.api.Assertions.*;

import io.cascadestore.lsm.benchmark.ycsb.CascadeStoreYcsbClient;
import io.cascadestore.lsm.benchmark.ycsb.CascadeStoreYcsbFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.Status;

class CascadeStoreYcsbClientTest {

  @TempDir Path tempDir;

  private CascadeStoreYcsbClient client;

  @BeforeEach
  void setUp() throws Exception {
    Properties properties = CascadeStoreYcsbFactory.exampleProperties();
    properties.setProperty(CascadeStoreYcsbFactory.PROP_DATADIR, tempDir.resolve("data").toString());
    properties.setProperty(CascadeStoreYcsbFactory.PROP_COMPACTION_STRATEGY, "THRESHOLD");

    client = new CascadeStoreYcsbClient();
    client.setProperties(properties);
    client.init();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (client != null) {
      client.cleanup();
    }
    CascadeStoreYcsbClient.resetSharedStoresForTests();
  }

  @Test
  void readThenUpdateUsesReadThroughCache() {
    Map<String, ByteIterator> insertValues = new HashMap<>();
    insertValues.put("field0", new ByteArrayByteIterator("alpha".getBytes()));
    insertValues.put("field1", new ByteArrayByteIterator("beta".getBytes()));
    assertEquals(Status.OK, client.insert("usertable", "user1", insertValues));

    Map<String, ByteIterator> readResult = new HashMap<>();
    assertEquals(Status.OK, client.read("usertable", "user1", null, readResult));

    Map<String, ByteIterator> updateValues = new HashMap<>();
    updateValues.put("field1", new ByteArrayByteIterator("gamma".getBytes()));
    assertEquals(Status.OK, client.update("usertable", "user1", updateValues));

    Map<String, ByteIterator> fullRecord = new HashMap<>();
    assertEquals(Status.OK, client.read("usertable", "user1", null, fullRecord));
    assertEquals("alpha", fullRecord.get("field0").toString());
    assertEquals("gamma", fullRecord.get("field1").toString());
  }

  @Test
  void insertReadUpdateDeleteRoundTrip() {
    Map<String, ByteIterator> insertValues = new HashMap<>();
    insertValues.put("field0", new ByteArrayByteIterator("alpha".getBytes()));
    insertValues.put("field1", new ByteArrayByteIterator("beta".getBytes()));

    assertEquals(Status.OK, client.insert("usertable", "user1", insertValues));

    Map<String, ByteIterator> readResult = new HashMap<>();
    Set<String> fields = new HashSet<>();
    fields.add("field0");
    assertEquals(Status.OK, client.read("usertable", "user1", fields, readResult));
    assertEquals("alpha", readResult.get("field0").toString());

    Map<String, ByteIterator> updateValues = new HashMap<>();
    updateValues.put("field1", new ByteArrayByteIterator("gamma".getBytes()));
    assertEquals(Status.OK, client.update("usertable", "user1", updateValues));

    Map<String, ByteIterator> fullRecord = new HashMap<>();
    assertEquals(Status.OK, client.read("usertable", "user1", null, fullRecord));
    assertEquals(2, fullRecord.size());
    assertEquals("gamma", fullRecord.get("field1").toString());

    assertEquals(Status.OK, client.delete("usertable", "user1"));
    assertEquals(Status.NOT_FOUND, client.read("usertable", "user1", null, new HashMap<>()));
  }

  @Test
  void scanReturnsInsertedRecords() {
    Map<String, ByteIterator> rowA = Map.of("field0", new ByteArrayByteIterator("a".getBytes()));
    Map<String, ByteIterator> rowB = Map.of("field0", new ByteArrayByteIterator("b".getBytes()));

    assertEquals(Status.OK, client.insert("usertable", "user0000000001", rowA));
    assertEquals(Status.OK, client.insert("usertable", "user0000000002", rowB));

    java.util.Vector<HashMap<String, ByteIterator>> scanResult = new java.util.Vector<>();
    assertEquals(Status.OK, client.scan("usertable", "user0000000001", 10, null, scanResult));
    assertEquals(2, scanResult.size());
  }

  @Test
  void sharedStoreAcrossMultipleClients() throws Exception {
    Path dataDir = tempDir.resolve("shared-data");
    Properties properties = CascadeStoreYcsbFactory.exampleProperties();
    properties.setProperty(CascadeStoreYcsbFactory.PROP_DATADIR, dataDir.toString());
    properties.setProperty(CascadeStoreYcsbFactory.PROP_RESET_DATADIR, "true");

    CascadeStoreYcsbClient first = new CascadeStoreYcsbClient();
    first.setProperties(properties);
    first.init();

    CascadeStoreYcsbClient second = new CascadeStoreYcsbClient();
    second.setProperties(properties);
    second.init();

    Map<String, ByteIterator> values = Map.of("field0", new ByteArrayByteIterator("shared".getBytes()));
    assertEquals(Status.OK, first.insert("usertable", "user1", values));

    Map<String, ByteIterator> readResult = new HashMap<>();
    assertEquals(Status.OK, second.read("usertable", "user1", null, readResult));
    assertEquals("shared", readResult.get("field0").toString());

    second.cleanup();
    assertEquals(Status.OK, first.read("usertable", "user1", null, new HashMap<>()));

    first.cleanup();
  }

  @Test
  void initCreatesFreshDataDirectory() throws Exception {
    Path dataDir = tempDir.resolve("fresh-data");
    Files.createDirectories(dataDir);
    Files.createFile(dataDir.resolve("stale.txt"));

    Properties properties = CascadeStoreYcsbFactory.exampleProperties();
    properties.setProperty(CascadeStoreYcsbFactory.PROP_DATADIR, dataDir.toString());
    properties.setProperty(CascadeStoreYcsbFactory.PROP_RESET_DATADIR, "true");

    CascadeStoreYcsbClient freshClient = new CascadeStoreYcsbClient();
    freshClient.setProperties(properties);
    freshClient.init();
    freshClient.cleanup();

    assertTrue(Files.isDirectory(dataDir));
    assertFalse(Files.exists(dataDir.resolve("stale.txt")));
  }

  @Test
  void shardedClientsRouteKeysToIndependentStores() throws Exception {
    Path dataDir = tempDir.resolve("sharded-data");
    Properties properties = CascadeStoreYcsbFactory.exampleProperties();
    properties.setProperty(CascadeStoreYcsbFactory.PROP_DATADIR, dataDir.toString());
    properties.setProperty(CascadeStoreYcsbFactory.PROP_RESET_DATADIR, "true");
    properties.setProperty(CascadeStoreYcsbFactory.PROP_SHARDS, "4");

    CascadeStoreYcsbClient writer = new CascadeStoreYcsbClient();
    writer.setProperties(properties);
    writer.init();

    assertTrue(Files.isDirectory(dataDir.resolve("shard-0")));
    assertTrue(Files.isDirectory(dataDir.resolve("shard-3")));

    CascadeStoreYcsbClient reader = new CascadeStoreYcsbClient();
    reader.setProperties(properties);
    reader.init();

    Map<String, ByteIterator> firstValues =
        Map.of("field0", new ByteArrayByteIterator("one".getBytes()));
    Map<String, ByteIterator> secondValues =
        Map.of("field0", new ByteArrayByteIterator("one".getBytes()));
    assertEquals(Status.OK, writer.insert("usertable", "user0000000001", firstValues));
    assertEquals(Status.OK, writer.insert("usertable", "user0000000002", secondValues));

    Map<String, ByteIterator> first = new HashMap<>();
    Map<String, ByteIterator> second = new HashMap<>();
    assertEquals(Status.OK, reader.read("usertable", "user0000000001", null, first));
    assertEquals(Status.OK, reader.read("usertable", "user0000000002", null, second));
    assertEquals("one", first.get("field0").toString());
    assertEquals("one", second.get("field0").toString());

    reader.cleanup();
    writer.cleanup();
  }

  @Test
  void shardedScanMergesAcrossShards() throws Exception {
    Properties properties = CascadeStoreYcsbFactory.exampleProperties();
    properties.setProperty(CascadeStoreYcsbFactory.PROP_DATADIR, tempDir.resolve("scan-data").toString());
    properties.setProperty(CascadeStoreYcsbFactory.PROP_SHARDS, "4");

    CascadeStoreYcsbClient client = new CascadeStoreYcsbClient();
    client.setProperties(properties);
    client.init();

    assertEquals(
        Status.OK,
        client.insert(
            "usertable",
            "user0000000001",
            Map.of("field0", new ByteArrayByteIterator("a".getBytes()))));
    assertEquals(
        Status.OK,
        client.insert(
            "usertable",
            "user0000000002",
            Map.of("field0", new ByteArrayByteIterator("b".getBytes()))));

    java.util.Vector<HashMap<String, ByteIterator>> scanResult = new java.util.Vector<>();
    assertEquals(Status.OK, client.scan("usertable", "user0000000001", 10, null, scanResult));
    assertEquals(2, scanResult.size());
    client.cleanup();
  }
}
