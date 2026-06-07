package io.cascadestore.lsm.io.compression;

import java.util.Arrays;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/** LZ4 compress/decompress helpers for SSTable value payloads. */
public final class Lz4Compression {

  private static final LZ4Factory LZ4 = LZ4Factory.fastestInstance();
  private static final LZ4Compressor COMPRESSOR = LZ4.fastCompressor();
  private static final LZ4FastDecompressor DECOMPRESSOR = LZ4.fastDecompressor();

  /** Do not compress tiny values where metadata overhead dominates. */
  public static final int MIN_COMPRESS_BYTES = 64;

  /** Require at least 5% savings before storing compressed bytes. */
  public static final double MIN_SAVINGS_RATIO = 0.95;

  private Lz4Compression() {}

  /**
   * Compresses {@code input} when beneficial.
   *
   * @return compressed bytes, or {@code null} when storing raw bytes is smaller/faster
   */
  public static byte[] compressIfBeneficial(byte[] input) {
    if (input == null || input.length < MIN_COMPRESS_BYTES) {
      return null;
    }

    int maxCompressedLength = COMPRESSOR.maxCompressedLength(input.length);
    byte[] compressed = new byte[maxCompressedLength];
    int compressedLength = COMPRESSOR.compress(input, 0, input.length, compressed, 0, maxCompressedLength);
    if (compressedLength >= input.length * MIN_SAVINGS_RATIO) {
      return null;
    }
    return Arrays.copyOf(compressed, compressedLength);
  }

  public static byte[] decompress(byte[] compressed, int uncompressedLength) {
    byte[] restored = new byte[uncompressedLength];
    DECOMPRESSOR.decompress(compressed, 0, restored, 0, uncompressedLength);
    return restored;
  }
}
