package org.apache.hadoop.ozone.common;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;

public class ChecksumByteBufferImpl implements ChecksumByteBuffer {

  public static class Java9Crc32CFactory {
    private static final MethodHandle NEW_CRC32C_MH;

    static {
      MethodHandle newCRC32C = null;
      try {
        newCRC32C = MethodHandles.publicLookup()
            .findConstructor(
                Class.forName("java.util.zip.CRC32C"),
                MethodType.methodType(void.class)
            );
      } catch (ReflectiveOperationException e) {
        // Should not reach here.
        throw new RuntimeException(e);
      }
      NEW_CRC32C_MH = newCRC32C;
    }

    public static java.util.zip.Checksum createChecksum() {
      try {
        // Should throw nothing
        return (Checksum) NEW_CRC32C_MH.invoke();
      } catch (Throwable t) {
        throw (t instanceof RuntimeException) ? (RuntimeException) t
            : new RuntimeException(t);
      }
    }
  };

  private Checksum checksum;

  public ChecksumByteBufferImpl(Checksum impl) {
    this.checksum = impl;
  }

  @Override
  public void update(ByteBuffer buffer) {
    if (buffer.hasArray()) {
      checksum.update(buffer.array(), buffer.position() + buffer.arrayOffset(),
          buffer.remaining());
    } else {
      byte[] b = new byte[buffer.remaining()];
      buffer.get(b);
      checksum.update(b, 0, b.length);
    }
  }

  @Override
  public void update(byte[] b, int off, int len) {
    checksum.update(b, off, len);
  }

  @Override
  public void update(int i) {
    checksum.update(i);
  }

  @Override
  public long getValue() {
    return checksum.getValue();
  }

  @Override
  public void reset() {
    checksum.reset();
  }

}