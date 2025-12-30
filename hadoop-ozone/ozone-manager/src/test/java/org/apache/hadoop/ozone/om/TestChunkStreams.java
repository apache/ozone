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

package org.apache.hadoop.ozone.om;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.storage.BlockInputStream;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.junit.jupiter.api.Test;

/**
 * This class tests KeyInputStream and KeyOutputStream.
 */
public class TestChunkStreams {

  private OzoneConfiguration conf = new OzoneConfiguration();

  @Test
  public void testReadGroupInputStream() throws Exception {
    String dataString = RandomStringUtils.secure().nextAscii(500);
    try (KeyInputStream groupInputStream =
             new KeyInputStream("key", createInputStreams(dataString))) {

      byte[] resBuf = new byte[500];
      int len = groupInputStream.read(resBuf, 0, 500);

      assertEquals(500, len);
      assertEquals(dataString, new String(resBuf, UTF_8));
    }
  }

  @Test
  public void testErrorReadGroupInputStream() throws Exception {
    String dataString = RandomStringUtils.secure().nextAscii(500);
    try (KeyInputStream groupInputStream =
             new KeyInputStream("key", createInputStreams(dataString))) {
      byte[] resBuf = new byte[600];
      // read 300 bytes first
      int len = groupInputStream.read(resBuf, 0, 340);
      assertEquals(3, groupInputStream.getCurrentStreamIndex());
      assertEquals(60, groupInputStream.getRemainingOfIndex(3));
      assertEquals(340, len);
      assertEquals(dataString.substring(0, 340),
          new String(resBuf, UTF_8).substring(0, 340));

      // read following 300 bytes, but only 200 left
      len = groupInputStream.read(resBuf, 340, 260);
      assertEquals(4, groupInputStream.getCurrentStreamIndex());
      assertEquals(0, groupInputStream.getRemainingOfIndex(4));
      assertEquals(160, len);
      assertEquals(dataString, new String(resBuf, UTF_8).substring(0, 500));

      // further read should get EOF
      len = groupInputStream.read(resBuf, 0, 1);
      // reached EOF, further read should get -1
      assertEquals(-1, len);
    }
  }

  @Nonnull
  private List<BlockInputStream> createInputStreams(String dataString) throws IOException {
    byte[] buf = dataString.getBytes(UTF_8);
    List<BlockInputStream> streams = new ArrayList<>();
    int offset = 0;
    for (int i = 0; i < 5; i++) {
      BlockInputStream in = createStream(buf, offset);
      offset += 100;
      streams.add(in);
    }
    return streams;
  }

  private BlockInputStream createStream(byte[] buf, int offset) throws IOException {
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    return new BlockInputStream(null, 100L, null, null, null,
        clientConfig) {
      private long pos;
      private final ByteArrayInputStream in =
          new ByteArrayInputStream(buf, offset, 100);

      @Override
      public synchronized void seek(long pos) {
        throw new UnsupportedOperationException();
      }

      @Override
      public synchronized long getPos() {
        return pos;
      }

      @Override
      public synchronized boolean seekToNewSource(long targetPos) {
        throw new UnsupportedOperationException();
      }

      @Override
      public synchronized int read() {
        return in.read();
      }

      @Override
      public synchronized int read(byte[] b, int off, int len) {
        int readLen = in.read(b, off, len);
        pos += readLen;
        return readLen;
      }
    };

  }
}
