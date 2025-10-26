package io.cascadestore.lsm.api;

import java.util.Arrays;

public class ByteArrayWrapper implements Comparable<ByteArrayWrapper> {
  private final byte[] data;

  public ByteArrayWrapper(byte[] data) {
    if (data == null) {
      throw new NullPointerException("Data cannot be null");
    }
    this.data = data;
  }

  public byte[] getData() {
    return data;
  }

  @Override
  public int compareTo(ByteArrayWrapper other) {
    if (other == null) {
      return 1;
    }

    byte[] otherData = other.getData();
    int length = Math.min(data.length, otherData.length);

    for (int i = 0; i < length; i++) {
      int a = data[i] & 0xff;
      int b = otherData[i] & 0xff;
      if (a != b) {
        return a - b;
      }
    }

    return data.length - otherData.length;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ByteArrayWrapper other = (ByteArrayWrapper) obj;
    return Arrays.equals(data, other.data);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(data);
  }

  @Override
  public String toString() {
    return "ByteArrayWrapper{" + "data=" + Arrays.toString(data) + '}';
  }
}
