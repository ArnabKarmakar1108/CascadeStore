package io.cascadestore.lsm.sstable.iterator;

import io.cascadestore.lsm.api.KeyValueIterator;
import io.cascadestore.lsm.sstable.SSTableEntry;
import io.cascadestore.lsm.sstable.SSTableIterator;
import java.io.IOException;

public interface SSTableIteratorFactory {

  SSTableIterator createInMemoryIterator(SSTableEntry[] entries);

  SSTableIterator createFileIterator(byte[] startKey, byte[] endKey) throws IOException;

  KeyValueIterator createIterator(byte[] startKey, byte[] endKey) throws IOException;
}
