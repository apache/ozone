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

package org.apache.hadoop.ozone.common;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.util.NativeCRC32Wrapper;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.PureJavaCrc32C;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify that different checksum implementations compute the same
 * results.
 */
public class TestChecksumImplsComputeSameValues {

  private static final int DATA_SIZE = 1024 * 1024 * 64;
  private ByteBuffer data = ByteBuffer.allocate(DATA_SIZE);
  private int[] bytesPerChecksum = {512, 1024, 2048, 4096, 32768, 1048576};

  @Test
  public void testCRC32ImplsMatch() {
    data.clear();
    data.put(RandomUtils.secure().randomBytes(data.remaining()));
    for (int bpc : bytesPerChecksum) {
      List<ChecksumByteBuffer> impls = new ArrayList<>();
      impls.add(new PureJavaCrc32ByteBuffer());
      impls.add(new ChecksumByteBufferImpl(new PureJavaCrc32()));
      impls.add(new ChecksumByteBufferImpl(new CRC32()));
      if (NativeCRC32Wrapper.isAvailable()) {
        impls.add(new ChecksumByteBufferImpl(new NativeCheckSumCRC32(1, bpc)));
      }
      assertTrue(validateImpls(data, impls, bpc));
    }
  }

  @Test
  public void testCRC32CImplsMatch() {
    data.clear();
    data.put(RandomUtils.secure().randomBytes(data.remaining()));
    for (int bpc : bytesPerChecksum) {
      List<ChecksumByteBuffer> impls = new ArrayList<>();
      impls.add(new PureJavaCrc32CByteBuffer());
      impls.add(new ChecksumByteBufferImpl(new PureJavaCrc32C()));
      try {
        impls.add(new ChecksumByteBufferImpl(
            ChecksumByteBufferFactory.Java9Crc32CFactory.createChecksum()));
      } catch (Throwable e) {
        // NOOP
      }
      // impls.add(new ChecksumByteBufferImpl(new CRC32C())));
      if (NativeCRC32Wrapper.isAvailable()) {
        impls.add(new ChecksumByteBufferImpl(new NativeCheckSumCRC32(2, bpc)));
      }
      assertTrue(validateImpls(data, impls, bpc));
    }
  }

  private boolean validateImpls(ByteBuffer buf, List<ChecksumByteBuffer> impls,
      int bpc) {
    for (int i = 0; i < buf.capacity(); i += bpc) {
      buf.position(i);
      buf.limit(i + bpc);
      impls.get(0).update(buf);
      int res = (int) impls.get(0).getValue();
      impls.get(0).reset();
      for (int j = 1; j < impls.size(); j++) {
        ChecksumByteBuffer csum = impls.get(j);
        buf.position(i);
        buf.limit(i + bpc);
        csum.update(buf);
        if ((int) csum.getValue() != res) {
          return false;
        }
        csum.reset();
      }
    }
    return true;
  }

}
