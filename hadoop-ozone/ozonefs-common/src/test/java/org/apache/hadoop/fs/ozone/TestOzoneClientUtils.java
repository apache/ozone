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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for OzoneClientUtils.
 */
public class TestOzoneClientUtils {
  @Test(expected = IllegalArgumentException.class)
  public void testNegativeLength() throws IOException {
    OzoneVolume volume = mock(OzoneVolume.class);
    OzoneBucket bucket = mock(OzoneBucket.class);
    String keyName = "dummy";
    ClientProtocol clientProtocol = mock(ClientProtocol.class);
    OzoneClientUtils.getFileChecksumWithCombineMode(volume, bucket, keyName,
        -1, OzoneClientConfig.ChecksumCombineMode.MD5MD5CRC, clientProtocol);

  }

  @Test
  public void testEmptyKeyName() throws IOException {
    OzoneVolume volume = mock(OzoneVolume.class);
    OzoneBucket bucket = mock(OzoneBucket.class);
    String keyName = "";
    ClientProtocol clientProtocol = mock(ClientProtocol.class);
    FileChecksum checksum =
        OzoneClientUtils.getFileChecksumWithCombineMode(volume, bucket, keyName,
            1, OzoneClientConfig.ChecksumCombineMode.MD5MD5CRC,
            clientProtocol);

    assertNull(checksum);
  }
}