/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Supplier;
import org.apache.hadoop.fs.Syncable;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.function.CheckedConsumer;
import org.apache.ratis.util.function.CheckedFunction;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test {@link SelectorOutputStream}.
 */
class TestSelectorOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestSelectorOutputStream.class);

  enum Op {
    FLUSH(SelectorOutputStream::flush),
    HFLUSH(SelectorOutputStream::hflush),
    HSYNC(SelectorOutputStream::hsync),
    CLOSE(SelectorOutputStream::close);

    private final CheckedConsumer<SelectorOutputStream<?>, IOException> method;

    Op(CheckedConsumer<SelectorOutputStream<?>, IOException> method) {
      this.method = method;
    }

    void accept(SelectorOutputStream<OutputStream> out) throws IOException {
      method.accept(out);
    }
  }

  static class SyncableOutputStreamForTesting
      extends ByteArrayOutputStream implements Syncable {
    @Override
    public void hflush() {
      LOG.info("hflush");
    }

    @Override
    public void hsync() {
      LOG.info("hsync");
    }
  }

  static Supplier<OutputStream> getOutputStreamSupplier(boolean isSyncable) {
    return isSyncable ? SyncableOutputStreamForTesting::new
        : ByteArrayOutputStream::new;
  }

  static void runTestSelector(int threshold, int byteToWrite,
      Op op) throws Exception {
    runTestSelector(threshold, byteToWrite, op, false);
  }

  static void runTestSelector(int threshold, int byteToWrite,
      Op op, boolean isSyncable) throws Exception {
    LOG.info("run: threshold={}, byteToWrite={}, op={}, isSyncable? {}",
        threshold, byteToWrite, op, isSyncable);
    final MemoizedSupplier<OutputStream> belowThreshold
        = MemoizedSupplier.valueOf(getOutputStreamSupplier(isSyncable));
    final MemoizedSupplier<OutputStream> aboveThreshold
        = MemoizedSupplier.valueOf(getOutputStreamSupplier(isSyncable));
    final CheckedFunction<Integer, OutputStream, IOException> selector
        = byteWritten -> byteWritten <= threshold ?
        belowThreshold.get() : aboveThreshold.get();

    final SelectorOutputStream<OutputStream> out = new SelectorOutputStream<>(
        threshold, selector);
    for (int i = 0; i < byteToWrite; i++) {
      out.write(i);
    }

    // checkout auto selection
    final boolean isAbove = byteToWrite > threshold;
    assertFalse(belowThreshold.isInitialized());
    assertEquals(isAbove, aboveThreshold.isInitialized());

    final boolean isBelow = !isAbove;
    if (op != null) {
      op.accept(out);
      assertEquals(isBelow, belowThreshold.isInitialized());
      assertEquals(isAbove, aboveThreshold.isInitialized());
    }
  }

  @Test
  void testFlush() throws Exception {
    runTestSelector(10, 2, Op.FLUSH);
    runTestSelector(10, 10, Op.FLUSH);
    runTestSelector(10, 20, Op.FLUSH);
  }

  @Test
  void testClose() throws Exception {
    runTestSelector(10, 2, Op.CLOSE);
    runTestSelector(10, 10, Op.CLOSE);
    runTestSelector(10, 20, Op.CLOSE);
  }

  @Test
  void testHflushSyncable() throws Exception {
    runTestSelector(10, 2, Op.HFLUSH, true);
    runTestSelector(10, 10, Op.HFLUSH, true);
    runTestSelector(10, 20, Op.HFLUSH, true);
  }

  @Test
  void testHflushNonSyncable() {
    final IllegalStateException thrown = assertThrows(
        IllegalStateException.class,
        () -> runTestSelector(10, 2, Op.HFLUSH, false));
    LOG.info("thrown", thrown);
    assertThat(thrown).hasMessageContaining("not Syncable");
  }

  @Test
  void testHSyncSyncable() throws Exception {
    runTestSelector(10, 2, Op.HSYNC, true);
    runTestSelector(10, 10, Op.HSYNC, true);
    runTestSelector(10, 20, Op.HSYNC, true);
  }

  @Test
  void testHSyncNonSyncable() {
    final IllegalStateException thrown = assertThrows(
        IllegalStateException.class,
        () -> runTestSelector(10, 2, Op.HSYNC, false));
    LOG.info("thrown", thrown);
    assertThat(thrown).hasMessageContaining("not Syncable");
  }
}
