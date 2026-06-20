package io.cascadestore.lsm.sstable;

import io.cascadestore.lsm.io.BufferedDataReader;
import io.cascadestore.lsm.io.ValueBufferPool;
import io.cascadestore.lsm.io.compression.Lz4Compression;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/** On-disk SSTable data-file header and per-record value encoding. */
final class SSTableDataFormat {

  static final int MAGIC = 0x4353414B; // "CASK"
  static final int VERSION_LEGACY = 1;
  static final int VERSION_LZ4 = 2;

  static final int LEGACY_HEADER_SIZE = 16;
  static final int LZ4_HEADER_SIZE = 32;

  static final byte FLAG_RAW = 0;
  static final byte FLAG_LZ4 = 1;

  private final int dataStartOffset;
  private final boolean lz4Values;

  private SSTableDataFormat(int dataStartOffset, boolean lz4Values) {
    this.dataStartOffset = dataStartOffset;
    this.lz4Values = lz4Values;
  }

  static SSTableDataFormat legacy() {
    return new SSTableDataFormat(LEGACY_HEADER_SIZE, false);
  }

  static SSTableDataFormat lz4() {
    return new SSTableDataFormat(LZ4_HEADER_SIZE, true);
  }

  int dataStartOffset() {
    return dataStartOffset;
  }

  boolean lz4Values() {
    return lz4Values;
  }

  static SSTableDataFormat readHeader(FileChannel channel) throws IOException {
    ByteBuffer probe = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
    if (channel.read(probe, 0) < 4) {
      return legacy();
    }
    probe.flip();
    if (probe.getInt() != MAGIC) {
      return legacy();
    }

    ByteBuffer header = ByteBuffer.allocate(LZ4_HEADER_SIZE - 4).order(ByteOrder.BIG_ENDIAN);
    channel.read(header, 4);
    header.flip();
    int version = header.getInt();
    header.getInt(); // compression type — only LZ4 supported today
    if (version != VERSION_LZ4) {
      throw new IOException("Unsupported SSTable data format version: " + version);
    }
    return lz4();
  }

  void writeHeader(FileChannel channel, long creationTime, int level) throws IOException {
    if (!lz4Values) {
      ByteBuffer header = ByteBuffer.allocate(LEGACY_HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
      header.putLong(creationTime);
      header.putInt(level);
      header.putInt(0); // entry count patched after flush
      header.flip();
      channel.write(header);
      return;
    }

    ByteBuffer header = ByteBuffer.allocate(LZ4_HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
    header.putInt(MAGIC);
    header.putInt(VERSION_LZ4);
    header.putInt(1); // LZ4
    header.putLong(creationTime);
    header.putInt(level);
    header.putInt(0); // entry count patched after flush
    header.putInt(0); // reserved
    header.flip();
    channel.write(header);
  }

  void patchEntryCount(FileChannel channel, int entryCount) throws IOException {
    int offset = lz4Values ? 24 : 12;
    ByteBuffer count = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
    count.putInt(entryCount);
    count.flip();
    channel.write(count, offset);
  }

  int readEntryCount(FileChannel channel) throws IOException {
    int offset = lz4Values ? 24 : 12;
    ByteBuffer count = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
    channel.read(count, offset);
    count.flip();
    return count.getInt();
  }

  void writeValueRecord(FileChannel channel, byte[] value, long expirationTime) throws IOException {
    if (value == null) {
      ByteBuffer tombstone = ByteBuffer.allocate(12).order(ByteOrder.BIG_ENDIAN);
      tombstone.putInt(0);
      tombstone.putLong(expirationTime);
      tombstone.flip();
      channel.write(tombstone);
      return;
    }

    byte[] compressed = lz4Values ? Lz4Compression.compressIfBeneficial(value) : null;
    if (!lz4Values) {
      ByteBuffer raw = ByteBuffer.allocate(4 + value.length + 8).order(ByteOrder.BIG_ENDIAN);
      raw.putInt(value.length);
      raw.put(value);
      raw.putLong(expirationTime);
      raw.flip();
      channel.write(raw);
      return;
    }
    if (compressed == null) {
      ByteBuffer raw =
          ByteBuffer.allocate(1 + 4 + value.length + 8).order(ByteOrder.BIG_ENDIAN);
      raw.putInt(value.length + 1);
      raw.put(FLAG_RAW);
      raw.put(value);
      raw.putLong(expirationTime);
      raw.flip();
      channel.write(raw);
      return;
    }

    ByteBuffer compressedRecord =
        ByteBuffer.allocate(1 + 4 + 4 + compressed.length + 8).order(ByteOrder.BIG_ENDIAN);
    compressedRecord.putInt(1 + 4 + compressed.length);
    compressedRecord.put(FLAG_LZ4);
    compressedRecord.putInt(value.length);
    compressedRecord.put(compressed);
    compressedRecord.putLong(expirationTime);
    compressedRecord.flip();
    channel.write(compressedRecord);
  }

  byte[] readValue(BufferedDataReader reader) throws IOException {
    int payloadLength = reader.readInt();
    if (payloadLength == 0) {
      reader.skip(8);
      return null;
    }

    if (!lz4Values) {
      byte[] value = ValueBufferPool.readCopy(reader, payloadLength);
      reader.skip(8);
      return value;
    }

    byte flag = readByte(reader);
    if (flag == FLAG_RAW) {
      byte[] value = ValueBufferPool.readCopy(reader, payloadLength - 1);
      reader.skip(8);
      return value;
    }
    if (flag != FLAG_LZ4) {
      throw new IOException("Unknown SSTable compression flag: " + flag);
    }

    int uncompressedLength = reader.readInt();
    int compressedLength = payloadLength - 1 - Integer.BYTES;
    byte[] compressed = ValueBufferPool.readCopy(reader, compressedLength);
    reader.skip(8);
    return Lz4Compression.decompress(compressed, uncompressedLength);
  }

  /**
   * Reads a full record including its expiration timestamp. Used by compaction, which must preserve
   * TTLs and distinguish tombstones ({@code value == null}) from live values.
   */
  ValueRecord readRecord(BufferedDataReader reader) throws IOException {
    int payloadLength = reader.readInt();
    if (payloadLength == 0) {
      long expiration = reader.readLong();
      return new ValueRecord(null, expiration);
    }

    if (!lz4Values) {
      byte[] value = ValueBufferPool.readCopy(reader, payloadLength);
      long expiration = reader.readLong();
      return new ValueRecord(value, expiration);
    }

    byte flag = readByte(reader);
    if (flag == FLAG_RAW) {
      byte[] value = ValueBufferPool.readCopy(reader, payloadLength - 1);
      long expiration = reader.readLong();
      return new ValueRecord(value, expiration);
    }
    if (flag != FLAG_LZ4) {
      throw new IOException("Unknown SSTable compression flag: " + flag);
    }

    int uncompressedLength = reader.readInt();
    int compressedLength = payloadLength - 1 - Integer.BYTES;
    byte[] compressed = ValueBufferPool.readCopy(reader, compressedLength);
    long expiration = reader.readLong();
    return new ValueRecord(Lz4Compression.decompress(compressed, uncompressedLength), expiration);
  }

  /** Decoded record: {@code value == null} denotes a tombstone. */
  static final class ValueRecord {
    private final byte[] value;
    private final long expirationTime;

    ValueRecord(byte[] value, long expirationTime) {
      this.value = value;
      this.expirationTime = expirationTime;
    }

    byte[] value() {
      return value;
    }

    long expirationTime() {
      return expirationTime;
    }
  }

  void skipValue(BufferedDataReader reader) throws IOException {
    int payloadLength = reader.readInt();
    if (payloadLength == 0) {
      reader.skip(8);
      return;
    }
    if (!lz4Values) {
      reader.skip(payloadLength + 8L);
      return;
    }
    reader.skip(payloadLength + 8L);
  }

  boolean readValuePresence(BufferedDataReader reader) throws IOException {
    int payloadLength = reader.readInt();
    if (payloadLength == 0) {
      reader.skip(8);
      return false;
    }
    reader.skip(payloadLength + 8L);
    return true;
  }

  private static byte readByte(BufferedDataReader reader) throws IOException {
    byte[] one = new byte[1];
    reader.readFully(one);
    return one[0];
  }
}
