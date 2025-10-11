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

package org.apache.hadoop.ozone.container.common.helpers;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsUtils.REDACTED_STRING;
import static org.apache.hadoop.hdds.HddsUtils.processForDebug;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.ReadChunk;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getReadChunkResponse;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getDummyCommandRequestProto;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ByteStringConversion;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.ratis.thirdparty.com.google.protobuf.TextFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

/**
 * Test for {@link ContainerUtils}.
 */
public class TestContainerUtils {

  private OzoneConfiguration conf;

  @BeforeEach
  void setup(@TempDir File dir) {
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.toString());
  }

  @Test
  public void redactsDataBuffers() {
    // GIVEN
    final String junk = "junk";
    ContainerCommandRequestProto req = getDummyCommandRequestProto(ReadChunk);
    ChunkBuffer data = ChunkBuffer.wrap(ByteBuffer.wrap(junk.getBytes(UTF_8)));
    ContainerCommandResponseProto resp = getReadChunkResponse(req, data,
        ByteStringConversion::safeWrap);

    final String original = TextFormat.shortDebugString(resp);
    // WHEN
    final String processed = processForDebug(resp);

    // THEN
    final int j = original.indexOf(junk);
    final int r = processed.indexOf(REDACTED_STRING);

    assertEquals(j, r);
    assertEquals(original.substring(0, j), processed.substring(0, r));
    assertEquals(original.substring(j + junk.length()), processed.substring(r + REDACTED_STRING.length()));
  }

  @Test
  public void testTarName() throws IOException {
    long containerId = 100;
    String tarName = ContainerUtils.getContainerTarName(containerId);

    assertEquals(containerId,
        ContainerUtils.retrieveContainerIdFromTarName(tarName));
  }

  @Test
  public void testDatanodeIDPersistent(@TempDir File tempDir) throws Exception {
    // Generate IDs for testing
    DatanodeDetails id1 = randomDatanodeDetails();
    try (MockedStatic<InetAddress> mockedStaticInetAddress = mockStatic(InetAddress.class)) {
      InetAddress mockedInetAddress = mock(InetAddress.class);
      mockedStaticInetAddress.when(() -> InetAddress.getByName(id1.getHostName()))
          .thenReturn(mockedInetAddress);

      // If persisted ip address is different from resolved ip address,
      // DatanodeDetails should return the persisted ip address.
      // Upon validation of the ip address, DatanodeDetails should return the resolved ip address.
      when(mockedInetAddress.getHostAddress())
          .thenReturn("127.0.0.1");
      assertWriteReadWithChangedIpAddress(tempDir, id1);

      when(mockedInetAddress.getHostAddress())
          .thenReturn(id1.getIpAddress());

      id1.setPort(DatanodeDetails.newStandalonePort(1));
      assertWriteRead(tempDir, id1);

      // Add certificate serial  id.
      id1.setCertSerialId(String.valueOf(RandomUtils.secure().randomLong()));
      assertWriteRead(tempDir, id1);

      // Read should return an empty value if file doesn't exist
      File nonExistFile = new File(tempDir, "non_exist.id");
      assertThrows(IOException.class,
          () -> ContainerUtils.readDatanodeDetailsFrom(nonExistFile));

      // Read should fail if the file is malformed
      File malformedFile = new File(tempDir, "malformed.id");
      createMalformedIDFile(malformedFile);
      assertThrows(IOException.class,
          () -> ContainerUtils.readDatanodeDetailsFrom(malformedFile));

      // Test upgrade scenario - protobuf file instead of yaml
      File protoFile = new File(tempDir, "valid-proto.id");
      try (OutputStream out = Files.newOutputStream(protoFile.toPath())) {
        HddsProtos.DatanodeDetailsProto proto = id1.getProtoBufMessage();
        proto.writeTo(out);
      }
      assertDetailsEquals(id1, ContainerUtils.readDatanodeDetailsFrom(protoFile));

      id1.setInitialVersion(1);
      assertWriteRead(tempDir, id1);
    }
  }

  private void assertWriteRead(@TempDir File tempDir,
      DatanodeDetails details) throws IOException {
    // Write a single ID to the file and read it out
    File file = new File(tempDir, "valid-values.id");
    ContainerUtils.writeDatanodeDetailsTo(details, file, conf);

    DatanodeDetails read = ContainerUtils.readDatanodeDetailsFrom(file);

    assertDetailsEquals(details, read);
    assertEquals(details.getCurrentVersion(), read.getCurrentVersion());
  }

  private void assertWriteReadWithChangedIpAddress(@TempDir File tempDir,
      DatanodeDetails details) throws IOException {
    // Write a single ID to the file and read it out
    File file = new File(tempDir, "valid-values.id");
    ContainerUtils.writeDatanodeDetailsTo(details, file, conf);
    DatanodeDetails read = ContainerUtils.readDatanodeDetailsFrom(file);
    assertEquals(details.getIpAddress(), read.getIpAddress());
    read.validateDatanodeIpAddress();
    assertEquals("127.0.0.1", read.getIpAddress());
  }

  private void createMalformedIDFile(File malformedFile)
      throws IOException {
    DatanodeDetails id = randomDatanodeDetails();
    ContainerUtils.writeDatanodeDetailsTo(id, malformedFile, conf);

    try (OutputStream out = Files.newOutputStream(malformedFile.toPath())) {
      out.write("malformed".getBytes(StandardCharsets.UTF_8));
    }
  }

  private static void assertDetailsEquals(DatanodeDetails expected,
      DatanodeDetails actual) {
    assertEquals(expected, actual);
    assertEquals(expected.getCertSerialId(), actual.getCertSerialId());
    assertEquals(expected.getProtoBufMessage(), actual.getProtoBufMessage());
    assertEquals(expected.getInitialVersion(), actual.getInitialVersion());
    assertEquals(expected.getIpAddress(), actual.getIpAddress());
  }
}
