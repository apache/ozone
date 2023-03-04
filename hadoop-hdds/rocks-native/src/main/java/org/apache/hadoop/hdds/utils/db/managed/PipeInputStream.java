package org.apache.hadoop.hdds.utils.db.managed;

import org.apache.hadoop.hdds.utils.NativeLibraryLoader;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_LIBRARY_NAME;

public class PipeInputStream extends InputStream {

  static {
    NativeLibraryLoader.getInstance()
            .loadLibrary(ROCKS_TOOLS_NATIVE_LIBRARY_NAME);
  }
  byte[] byteBuffer;
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
    index+=1;
    return ret;
  }

  private native long newPipe();

  private native int readInternal(byte[] byteBuffer, int capacity,
                                  long nativeHandle);
  private native void closeInternal(long nativeHandle);

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

  public static void main(String[] args) throws IOException,
          NativeLibraryNotLoadedException {
    PipeInputStream p = new PipeInputStream(50);
    System.out.println(Arrays.toString(p.byteBuffer));
    BufferedReader bufferedReader =new BufferedReader(new InputStreamReader(p));
    char[] a = new char[5];
    int numberOfCharsRead = 0;
    StringBuilder stringBuilder =new StringBuilder();
    do {
      stringBuilder.append(a, 0, numberOfCharsRead);
      numberOfCharsRead = bufferedReader.read(a);
    }while(numberOfCharsRead >=0);
    p.readInternal(p.byteBuffer, p.capacity,p.nativeHandle);
    System.out.println(stringBuilder.toString());
    System.out.println(Arrays.toString(p.byteBuffer));

  }
}
