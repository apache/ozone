/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.shell.keys;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.CompositeCrcFileChecksum;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.util.CrcUtil;
import org.apache.hadoop.util.DataChecksum;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

/**
 * Tests for ChecksumKeyHandler.
 */
public class TestChecksumKeyHandler {

  private ChecksumKeyHandler cmd;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
  private static final int CHECKSUM = 123456;

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {

    cmd = new ChecksumKeyHandler() {
      // Just return a known checksum for testing, as it reduces the amount of
      // mocking needed.
      @Override
      protected FileChecksum getFileChecksum(OzoneVolume vol,
          OzoneBucket bucket, String keyName, long dataSize,
          ClientProtocol clientProxy) {
        return new CompositeCrcFileChecksum(
            CHECKSUM, DataChecksum.Type.CRC32, 512);
      }
    };
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testChecksumKeyHandler()
      throws OzoneClientException, IOException {
    OzoneAddress address = new OzoneAddress("o3://ozone1/volume/bucket/key");
    long keySize = 1024L;

    ObjectStore objectStore = mock(ObjectStore.class);
    OzoneClient client = mock(OzoneClient.class);
    Mockito.when(client.getObjectStore()).thenReturn(objectStore);

    OzoneVolume volume = mock(OzoneVolume.class);
    OzoneBucket bucket = mock(OzoneBucket.class);
    OzoneKeyDetails key = mock(OzoneKeyDetails.class);

    Mockito.when(volume.getBucket(anyString())).thenReturn(bucket);
    Mockito.when(bucket.getKey(anyString()))
        .thenReturn(key);
    Mockito.when(objectStore.getVolume(anyString())).
        thenReturn(volume);
    Mockito.when(key.getDataSize()).thenReturn(keySize);

    cmd.execute(client, address);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode json = mapper.readTree(outContent.toString("UTF-8"));
    Assertions.assertEquals("volume", json.get("volumeName").asText());
    Assertions.assertEquals("bucket", json.get("bucketName").asText());
    Assertions.assertEquals("key", json.get("name").asText());
    Assertions.assertEquals(keySize, json.get("dataSize").asLong());
    Assertions.assertEquals("COMPOSITE-CRC32", json.get("algorithm").asText());

    String expectedChecksum = javax.xml.bind.DatatypeConverter.printHexBinary(
        CrcUtil.intToBytes(Integer.valueOf(CHECKSUM)));
    Assertions.assertEquals(expectedChecksum, json.get("checksum").asText());
  }

}
