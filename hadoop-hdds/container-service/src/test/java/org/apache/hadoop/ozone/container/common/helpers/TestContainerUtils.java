/*
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
package org.apache.hadoop.ozone.container.common.helpers;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ByteStringConversion;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.ReadChunk;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getReadChunkResponse;
import static org.apache.hadoop.hdds.HddsUtils.processForDebug;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getDummyCommandRequestProto;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    ContainerCommandRequestProto req = getDummyCommandRequestProto(ReadChunk);
    ChunkBuffer data = ChunkBuffer.wrap(ByteBuffer.wrap(
        "junk".getBytes(UTF_8)));
    ContainerCommandResponseProto resp = getReadChunkResponse(req, data,
        ByteStringConversion::safeWrap);

    // WHEN
    ContainerCommandResponseProto processed = processForDebug(resp);

    // THEN
    ContainerProtos.DataBuffers dataBuffers =
        processed.getReadChunk().getDataBuffers();
    assertEquals(1, dataBuffers.getBuffersCount());
    assertEquals("<redacted>", dataBuffers.getBuffers(0).toString(UTF_8));
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
    id1.setPort(DatanodeDetails.newStandalonePort(1));
    assertWriteRead(tempDir, id1);

    // Add certificate serial  id.
    id1.setCertSerialId("" + RandomUtils.nextLong());
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
    try (FileOutputStream out = new FileOutputStream(protoFile)) {
      HddsProtos.DatanodeDetailsProto proto = id1.getProtoBufMessage();
      proto.writeTo(out);
    }
    assertDetailsEquals(id1, ContainerUtils.readDatanodeDetailsFrom(protoFile));

    id1.setInitialVersion(1);
    assertWriteRead(tempDir, id1);
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

  private void createMalformedIDFile(File malformedFile)
      throws IOException {
    DatanodeDetails id = randomDatanodeDetails();
    ContainerUtils.writeDatanodeDetailsTo(id, malformedFile, conf);

    try (FileOutputStream out = new FileOutputStream(malformedFile)) {
      out.write("malformed".getBytes(StandardCharsets.UTF_8));
    }
  }

  private static void assertDetailsEquals(DatanodeDetails expected,
      DatanodeDetails actual) {
    assertEquals(expected, actual);
    assertEquals(expected.getCertSerialId(), actual.getCertSerialId());
    assertEquals(expected.getProtoBufMessage(), actual.getProtoBufMessage());
    assertEquals(expected.getInitialVersion(), actual.getInitialVersion());
  }
}
