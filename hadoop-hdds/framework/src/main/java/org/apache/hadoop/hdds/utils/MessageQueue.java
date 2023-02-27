/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.utils;

import org.apache.commons.io.input.buffer.CircularByteBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Message Queue to Pipe output from one output stream to another inputstream.
 */
public class MessageQueue {
  private CircularByteBuffer byteBuffer;
  private AtomicBoolean isRunning;

  private MessageOutputStream messageOutputStream;
  private MessageInputStream messageInputStream;

  public MessageQueue(int bufferSize, long pollIntervalMillis) {
    this.pollIntervalMillis = pollIntervalMillis;
    init(bufferSize);
  }

  private void init(int bufferSize) {
    this.byteBuffer = new CircularByteBuffer(bufferSize);
    this.isRunning = new AtomicBoolean(false);
    this.messageInputStream = new MessageInputStream(this);
    this.messageOutputStream = new MessageOutputStream(this);
  }

  public void start() {
    this.isRunning.set(true);
  }

  public void stop() {
    this.isRunning.set(false);
  }

  public MessageOutputStream getMessageOutputStream() {
    return messageOutputStream;
  }

  public MessageInputStream getMessageInputStream() {
    return messageInputStream;
  }

  private long pollIntervalMillis;

  public boolean isRunning() {
    return isRunning.get();
  }

  private long getPollIntervalMillis() {
    return pollIntervalMillis;
  }

  private boolean hasSpace(int requiredLength) {
    return this.byteBuffer.hasSpace(requiredLength);
  }

  private boolean hasBytes() {
    return this.byteBuffer.hasBytes();
  }

  private int getCurrentNumberOfBytes() {
    return this.byteBuffer.getCurrentNumberOfBytes();
  }

  private void add(byte[] b, int off, int len) {
    this.byteBuffer.add(b, off, len);
  }

  private int read(byte[] b, int off, int len) {
    this.byteBuffer.read(b, off, len);
    return len;
  }

  private static  <T> T callWithLock(Lock lock, Callable<T> callable)
          throws Exception {
    lock.lock();
    try {
      return callable.call();
    } finally {
      lock.unlock();
    }
  }
  private static final class MessageOutputStream extends OutputStream {

    private MessageQueue messageQueue;
    private Lock writeLock;

    private MessageOutputStream(MessageQueue messageQueue) {
      this.messageQueue = messageQueue;
      this.writeLock = new ReentrantLock();

    }

    private void waitForBytes(int requiredLength) throws InterruptedException {
      while (!this.messageQueue.hasSpace(requiredLength)) {
        Thread.sleep(this.messageQueue.getPollIntervalMillis());
      }
    }



    @Override
    public void write(int b) throws IOException {
      this.write(new byte[]{(byte) b});
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      try {
        if (!this.messageQueue.isRunning()) {
          throw new IOException("Message Queue is Closed");
        }
        waitForBytes(len);
        callWithLock(this.writeLock, () -> {
          waitForBytes(len);
          this.messageQueue.add(b, off, len);
          return true;
        });
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }
  private static final class MessageInputStream extends InputStream {

    private MessageQueue messageQueue;
    private Lock readLock;

    private MessageInputStream(MessageQueue messageQueue) {
      this.messageQueue = messageQueue;
      this.readLock = new ReentrantLock();
    }

    private void waitForBytes() throws InterruptedException {
      while (!this.messageQueue.hasBytes() && this.messageQueue.isRunning()) {
        Thread.sleep(messageQueue.getPollIntervalMillis());
      }
    }

    @Override
    public int read() throws IOException {
      byte[] readByte = new byte[1];
      int numberOfBytesRead = this.read(readByte);
      return numberOfBytesRead == -1 ? -1 : (readByte[0] & 0xff);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      try {
        return MessageQueue.callWithLock(this.readLock, () -> {
          waitForBytes();
          if (!this.messageQueue.isRunning()) {
            return -1;
          }
          return this.messageQueue.read(b, off, Math.min(len,
                  this.messageQueue.getCurrentNumberOfBytes()));
        });
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }
}
