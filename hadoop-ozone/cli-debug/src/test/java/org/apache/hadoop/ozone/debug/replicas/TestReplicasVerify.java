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

package org.apache.hadoop.ozone.debug.replicas;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.shell.ShellReplicationOptions;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Unit tests for replicas verify command.
 */
class TestReplicasVerify {

  @Test
  void testRefreshContainerLocationsFromScmOption() throws Exception {
    ReplicasVerify command = new ReplicasVerify();

    new CommandLine(command).parseArgs("--checksums", "--refresh-from-scm", "/volume1");

    assertThat(isRefreshContainerLocationsFromScmEnabled(command)).isTrue();
  }

  @Test
  void testRefreshContainerLocationsFromScmDefault() throws Exception {
    ReplicasVerify command = new ReplicasVerify();

    new CommandLine(command).parseArgs("--checksums", "/volume1");

    assertThat(isRefreshContainerLocationsFromScmEnabled(command)).isFalse();
  }

  @Test
  void testRetriesWithForcedContainerCacheRefreshOnBlockNotFound() throws Exception {
    ReplicasVerify replicasVerify = new ReplicasVerify();
    ReplicaVerifier blockExistenceVerifier = mock(ReplicaVerifier.class);
    ReplicaVerifier checksumVerifier = mock(ReplicaVerifier.class);

    when(blockExistenceVerifier.getType()).thenReturn("blockExistence");
    when(checksumVerifier.getType()).thenReturn("checksum");
    when(blockExistenceVerifier.verifyBlock(any(), any()))
        .thenReturn(BlockVerificationResult.failCheckAndRefreshKeyLocation("Block not found"))
        .thenReturn(BlockVerificationResult.pass());
    when(checksumVerifier.verifyBlock(any(), any()))
        .thenReturn(BlockVerificationResult.pass())
        .thenReturn(BlockVerificationResult.pass());
    setField(replicasVerify, "replicaVerifiers", Arrays.asList(blockExistenceVerifier, checksumVerifier));
    setField(replicasVerify, "replication", emptyReplicationFilter());

    OzoneClient ozoneClient = mock(OzoneClient.class);
    ClientProtocol proxy = mock(ClientProtocol.class);
    when(ozoneClient.getProxy()).thenReturn(proxy);

    OmKeyLocationInfo firstLocation = createKeyLocationInfo(1L, 11L);
    OmKeyLocationInfo refreshedLocation = createKeyLocationInfo(2L, 22L);
    when(proxy.getKeyInfo("vol1", "bucket1", "key1", false))
        .thenReturn(createKeyInfo(firstLocation));
    when(proxy.getKeyInfo("vol1", "bucket1", "key1", true))
        .thenReturn(createKeyInfo(refreshedLocation));

    ObjectNode root = JsonUtils.createObjectNode(null);
    ArrayNode keysArray = root.putArray("keys");
    AtomicBoolean allKeysPassed = new AtomicBoolean(true);

    replicasVerify.processKey(ozoneClient, "vol1", "bucket1", "key1", keysArray, allKeysPassed);

    verify(proxy).getKeyInfo("vol1", "bucket1", "key1", false);
    verify(proxy).getKeyInfo("vol1", "bucket1", "key1", true);
    verify(blockExistenceVerifier, times(2)).verifyBlock(any(), any());
    verify(checksumVerifier, times(2)).verifyBlock(any(), any());
    assertTrue(allKeysPassed.get());
    assertEquals(0, keysArray.size());
  }

  @Test
  void testDoesNotRefreshOnNonRefreshableFailure() throws Exception {
    ReplicasVerify replicasVerify = new ReplicasVerify();
    ReplicaVerifier blockExistenceVerifier = mock(ReplicaVerifier.class);
    ReplicaVerifier checksumVerifier = mock(ReplicaVerifier.class);

    when(blockExistenceVerifier.getType()).thenReturn("blockExistence");
    when(checksumVerifier.getType()).thenReturn("checksum");
    when(blockExistenceVerifier.verifyBlock(any(), any()))
        .thenReturn(BlockVerificationResult.failCheck("Block does not exist on this replica"));
    when(checksumVerifier.verifyBlock(any(), any()))
        .thenReturn(BlockVerificationResult.pass());
    setField(replicasVerify, "replicaVerifiers", Arrays.asList(blockExistenceVerifier, checksumVerifier));
    setField(replicasVerify, "replication", emptyReplicationFilter());

    OzoneClient ozoneClient = mock(OzoneClient.class);
    ClientProtocol proxy = mock(ClientProtocol.class);
    when(ozoneClient.getProxy()).thenReturn(proxy);
    when(proxy.getKeyInfo("vol1", "bucket1", "key1", false))
        .thenReturn(createKeyInfo(createKeyLocationInfo(1L, 11L)));

    ObjectNode root = JsonUtils.createObjectNode(null);
    ArrayNode keysArray = root.putArray("keys");
    AtomicBoolean allKeysPassed = new AtomicBoolean(true);

    replicasVerify.processKey(ozoneClient, "vol1", "bucket1", "key1", keysArray, allKeysPassed);

    verify(proxy).getKeyInfo("vol1", "bucket1", "key1", false);
    verify(proxy, never()).getKeyInfo("vol1", "bucket1", "key1", true);
    verify(blockExistenceVerifier, times(1)).verifyBlock(any(), any());
    verify(checksumVerifier, times(1)).verifyBlock(any(), any());
    assertFalse(allKeysPassed.get());
    assertEquals(1, keysArray.size());
    assertFalse(keysArray.get(0).get("pass").asBoolean());
  }

  @Test
  void testRefreshFailureMarksKeyIncompleteAndDoesNotAbortRun() throws Exception {
    ReplicasVerify replicasVerify = new ReplicasVerify();
    ReplicaVerifier blockExistenceVerifier = mock(ReplicaVerifier.class);

    when(blockExistenceVerifier.getType()).thenReturn("blockExistence");
    when(blockExistenceVerifier.verifyBlock(any(), any()))
        .thenReturn(BlockVerificationResult.failCheckAndRefreshKeyLocation("Block not found"));
    setField(replicasVerify, "replicaVerifiers", Collections.singletonList(blockExistenceVerifier));
    setField(replicasVerify, "replication", emptyReplicationFilter());

    OzoneClient ozoneClient = mock(OzoneClient.class);
    ClientProtocol proxy = mock(ClientProtocol.class);
    when(ozoneClient.getProxy()).thenReturn(proxy);
    when(proxy.getKeyInfo("vol1", "bucket1", "key1", false))
        .thenReturn(createKeyInfo(createKeyLocationInfo(1L, 11L)));
    when(proxy.getKeyInfo("vol1", "bucket1", "key1", true))
        .thenThrow(new IOException("OM refresh failed"));

    ObjectNode root = JsonUtils.createObjectNode(null);
    ArrayNode keysArray = root.putArray("keys");
    AtomicBoolean allKeysPassed = new AtomicBoolean(true);

    replicasVerify.processKey(ozoneClient, "vol1", "bucket1", "key1", keysArray, allKeysPassed);

    verify(proxy).getKeyInfo("vol1", "bucket1", "key1", false);
    verify(proxy).getKeyInfo("vol1", "bucket1", "key1", true);
    assertFalse(allKeysPassed.get());
    assertEquals(1, keysArray.size());
    assertFalse(keysArray.get(0).get("pass").asBoolean());
    assertFalse(keysArray.get(0).get("blocks").get(0)
        .get("replicas").get(0).get("checks").get(0).get("completed").asBoolean());
    assertThat(keysArray.get(0).get("blocks").get(0)
        .get("replicas").get(0).get("checks").get(0).get("failures").toString())
        .contains("Failed to refresh key location from OM: OM refresh failed");
  }

  @Test
  void testInitialGetKeyInfoFailureMarksKeyIncompleteAndDoesNotAbortRun() throws Exception {
    ReplicasVerify replicasVerify = new ReplicasVerify();
    OzoneClient ozoneClient = mock(OzoneClient.class);
    ClientProtocol proxy = mock(ClientProtocol.class);
    when(ozoneClient.getProxy()).thenReturn(proxy);
    when(proxy.getKeyInfo("vol1", "bucket1", "key1", false))
        .thenThrow(new IOException("Key not found"));

    ObjectNode root = JsonUtils.createObjectNode(null);
    ArrayNode keysArray = root.putArray("keys");
    AtomicBoolean allKeysPassed = new AtomicBoolean(true);

    replicasVerify.processKey(ozoneClient, "vol1", "bucket1", "key1", keysArray, allKeysPassed);

    verify(proxy).getKeyInfo("vol1", "bucket1", "key1", false);
    verify(proxy, never()).getKeyInfo("vol1", "bucket1", "key1", true);
    assertFalse(allKeysPassed.get());
    assertEquals(1, keysArray.size());
    assertFalse(keysArray.get(0).get("completed").asBoolean());
    assertFalse(keysArray.get(0).get("pass").asBoolean());
    assertThat(keysArray.get(0).get("failures").toString())
        .contains("Failed to fetch key info from OM: Key not found");
  }

  private static boolean isRefreshContainerLocationsFromScmEnabled(ReplicasVerify command) throws Exception {
    Field field = ReplicasVerify.class.getDeclaredField("refreshContainerLocationsFromScm");
    field.setAccessible(true);
    return field.getBoolean(command);
  }

  private static OmKeyInfo createKeyInfo(OmKeyLocationInfo keyLocationInfo) {
    OmKeyLocationInfoGroup latestVersionLocations =
        new OmKeyLocationInfoGroup(0, Collections.singletonList(keyLocationInfo));
    return new OmKeyInfo.Builder()
        .setVolumeName("vol1")
        .setBucketName("bucket1")
        .setKeyName("key1")
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE))
        .setOmKeyLocationInfos(Collections.singletonList(latestVersionLocations))
        .build();
  }

  private static OmKeyLocationInfo createKeyLocationInfo(long containerId, long localId) {
    DatanodeDetails datanode = MockDatanodeDetails.randomDatanodeDetails();
    Pipeline pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE))
        .setState(Pipeline.PipelineState.OPEN)
        .setNodes(Collections.singletonList(datanode))
        .build();

    return new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(containerId, localId))
        .setPipeline(pipeline)
        .setLength(1)
        .setOffset(0)
        .setCreateVersion(0)
        .build();
  }

  private static ShellReplicationOptions emptyReplicationFilter() {
    ShellReplicationOptions replication = mock(ShellReplicationOptions.class);
    when(replication.fromParams(any())).thenReturn(Optional.empty());
    return replication;
  }

  private static void setField(Object target, String fieldName, Object value) throws Exception {
    Class<?> type = target.getClass();
    while (type != null) {
      try {
        Field field = type.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
        return;
      } catch (NoSuchFieldException ignored) {
        type = type.getSuperclass();
      }
    }
    throw new IOException("Unable to set field '" + fieldName + "' on " + target.getClass().getName());
  }
}
