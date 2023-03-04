package org.apache.hadoop.hdds.utils.db.managed;

import org.apache.hadoop.hdds.utils.NativeLibraryLoader;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_LIBRARY_NAME;

/**
 * JNI for reading data from pipe.
 */
public class PipeInputStream extends InputStream {

  static {
    NativeLibraryLoader.getInstance()
            .loadLibrary(ROCKS_TOOLS_NATIVE_LIBRARY_NAME);
  }
  private byte[] byteBuffer;
  private long nativeHandle;
  private int numberOfBytesLeftToRead;
  private int index = 0;
  private int capacity;

  private AtomicBoolean cleanup;

  PipeInputStream(int capacity) throws NativeLibraryNotLoadedException {
    if (!NativeLibraryLoader.getInstance()
            .isLibraryLoaded(ROCKS_TOOLS_NATIVE_LIBRARY_NAME)) {
      throw new NativeLibraryNotLoadedException(
              ROCKS_TOOLS_NATIVE_LIBRARY_NAME);
    }

    this.byteBuffer = new byte[capacity];
    this.numberOfBytesLeftToRead = 0;
    this.capacity = capacity;
    this.nativeHandle = newPipe();
    this.cleanup = new AtomicBoolean(false);
  }

  long getNativeHandle() {
    return nativeHandle;
  }

  @Override
  public int read() {
    if (numberOfBytesLeftToRead < 0) {
      this.close();
      return -1;
    }
    if (numberOfBytesLeftToRead == 0) {
      numberOfBytesLeftToRead = readInternal(byteBuffer, capacity,
              nativeHandle);
      index = 0;
      return read();
    }
    numberOfBytesLeftToRead--;
    int ret = byteBuffer[index] & 0xFF;
    index += 1;
    return ret;
  }

  private native long newPipe();

  private native int readInternal(byte[] buff, int numberOfBytes,
                                  long pipeHandle);
  private native void closeInternal(long pipeHandle);

  @Override
  public void close() {
    if (this.cleanup.compareAndSet(false, true)) {
      closeInternal(this.nativeHandle);
    }
  }

  @Override
  protected void finalize() throws Throwable {
    close();
    super.finalize();
  }
}
