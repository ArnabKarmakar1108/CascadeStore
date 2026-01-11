package io.cascadestore.lsm.core.compaction;

import io.cascadestore.lsm.sstable.SSTable;
import java.util.List;

public interface CompactionStrategy {

  boolean shouldCompact(List<SSTable> ssTables);

  List<SSTable> selectTableToCompact(List<SSTable> ssTables);

  int getCompactionOutputLevel(List<SSTable> tablesToCompact);

  String getName();
}
