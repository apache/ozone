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

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReconcileContainerCommandProto;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.junit.jupiter.api.Test;

/**
 * Test cases to verify {@link ReconcileContainerCommand} serialization.
 */
public class TestReconcileContainerCommand {

  @Test
  public void testApparentVersionRoundTrip() {
    ReconcileContainerCommand cmd = createCommand();
    cmd.setApparentVersion(HDDSVersion.ZDU);

    ReconcileContainerCommandProto proto = cmd.getProto();
    ReconcileContainerCommand deserialized = ReconcileContainerCommand.getFromProtobuf(proto);

    assertEquals(HDDSVersion.ZDU, deserialized.getApparentVersion());
  }

  @Test
  public void testLayoutFeatureApparentVersionRoundTrip() {
    // Pre-ZDU datanodes report a layout feature as their apparent version.
    // The type must survive serialization rather than being erased to an
    // HDDSVersion.
    ReconcileContainerCommand cmd = createCommand();
    cmd.setApparentVersion(HDDSLayoutFeature.HBASE_SUPPORT);

    ReconcileContainerCommandProto proto = cmd.getProto();
    ReconcileContainerCommand deserialized = ReconcileContainerCommand.getFromProtobuf(proto);

    assertEquals(HDDSLayoutFeature.HBASE_SUPPORT, deserialized.getApparentVersion());
  }

  @Test
  public void testApparentVersionDefaultWhenAbsent() {
    ReconcileContainerCommand cmd = createCommand();

    // Build a proto without the apparentVersion field to mimic an old server.
    ReconcileContainerCommandProto proto = ReconcileContainerCommandProto.newBuilder(cmd.getProto())
            .clearApparentVersion()
            .build();

    ReconcileContainerCommand deserialized = ReconcileContainerCommand.getFromProtobuf(proto);

    assertEquals(HDDSVersion.DEFAULT_VERSION, deserialized.getApparentVersion());
  }

  @Test
  public void testToStringIncludesApparentVersion() {
    ReconcileContainerCommand cmd = createCommand();
    cmd.setApparentVersion(HDDSVersion.SOFTWARE_VERSION);

    String str = cmd.toString();
    assertTrue(str.contains("apparentVersion=" + HDDSVersion.SOFTWARE_VERSION));
  }

  private ReconcileContainerCommand createCommand() {
    return new ReconcileContainerCommand(1L, ImmutableSet.of(
        MockDatanodeDetails.randomDatanodeDetails(),
        MockDatanodeDetails.randomDatanodeDetails()));
  }
}
