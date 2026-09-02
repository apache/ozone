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

package org.apache.hadoop.ozone.protocol.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import java.util.Collections;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReconstructECContainersCommandProto;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex;
import org.junit.jupiter.api.Test;

/**
 * Test cases to verify {@link ReconstructECContainersCommand} serialization.
 */
public class TestReconstructECContainersCommand {

  @Test
  public void testApparentVersionRoundTrip() {
    ReconstructECContainersCommand cmd = createCommand();
    cmd.setApparentVersion(HDDSVersion.ZDU);

    ReconstructECContainersCommandProto proto = cmd.getProto();
    ReconstructECContainersCommand deserialized =
        ReconstructECContainersCommand.getFromProtobuf(proto);

    assertEquals(HDDSVersion.ZDU, deserialized.getApparentVersion());
  }

  @Test
  public void testLayoutFeatureApparentVersionRoundTrip() {
    // Pre-ZDU datanodes report a layout feature as their apparent version.
    // The type must survive serialization rather than being erased to an
    // HDDSVersion.
    ReconstructECContainersCommand cmd = createCommand();
    cmd.setApparentVersion(HDDSLayoutFeature.HBASE_SUPPORT);

    ReconstructECContainersCommandProto proto = cmd.getProto();
    ReconstructECContainersCommand deserialized = ReconstructECContainersCommand.getFromProtobuf(proto);

    assertEquals(HDDSLayoutFeature.HBASE_SUPPORT, deserialized.getApparentVersion());
  }

  @Test
  public void testApparentVersionDefaultWhenAbsent() {
    ReconstructECContainersCommand cmd = createCommand();

    // Build a proto without the apparentVersion field to mimic an old server.
    ReconstructECContainersCommandProto proto = ReconstructECContainersCommandProto.newBuilder(cmd.getProto())
            .clearApparentVersion()
            .build();

    ReconstructECContainersCommand deserialized = ReconstructECContainersCommand.getFromProtobuf(proto);

    assertEquals(HDDSVersion.DEFAULT_VERSION, deserialized.getApparentVersion());
  }

  @Test
  public void testToStringIncludesApparentVersion() {
    ReconstructECContainersCommand cmd = createCommand();
    cmd.setApparentVersion(HDDSVersion.SOFTWARE_VERSION);

    String str = cmd.toString();
    assertTrue(str.contains("apparentVersion: " + HDDSVersion.SOFTWARE_VERSION));
  }

  private ReconstructECContainersCommand createCommand() {
    DatanodeDetails source = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();
    byte[] missingIndexes = new byte[]{2};
    return new ReconstructECContainersCommand(1L,
        Collections.singletonList(new DatanodeDetailsAndReplicaIndex(source, 1)),
        Collections.singletonList(target),
        ByteString.copyFrom(missingIndexes),
        new ECReplicationConfig(3, 2));
  }
}
