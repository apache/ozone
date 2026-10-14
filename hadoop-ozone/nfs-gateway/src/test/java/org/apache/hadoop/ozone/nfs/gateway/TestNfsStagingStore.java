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

package org.apache.hadoop.ozone.nfs.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

class TestNfsStagingStore {
  private static final String EXPORT_UUID = "f7ba65dc-87cf-4a5e-9136-c58f2b85ab12";
  private static final String TRANSFER_ID = "d89187e3-73a2-47bc-b7bc-a3145829bcbc";
  private final OzoneConfiguration configuration = new OzoneConfiguration();

  @Test
  void storesAndReplaysIdenticalWriteRangesUnderAnImmutableKey() throws Exception {
    RpcClient client = mock(RpcClient.class);
    OzoneOutputStream output = mock(OzoneOutputStream.class);
    byte[] data = "durable nfs range".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    OzoneBucket stagingBucket = stagingBucket();
    when(client.getBucketDetails("nfs-system", "staging")).thenReturn(stagingBucket);
    when(client.createFile(eq("nfs-system"), eq("staging"), anyString(), eq((long) data.length),
        any(ReplicationConfig.class), eq(false), eq(true)))
        .thenReturn(output).thenThrow(new OMException("exists", OMException.ResultCodes.KEY_ALREADY_EXISTS));
    when(client.readFile(eq("nfs-system"), eq("staging"), anyString()))
        .thenReturn(new OzoneInputStream(new ByteArrayInputStream(data)));
    NfsStagingStore store = new NfsStagingStore("nfs-system", "staging",
        UserGroupInformation.createRemoteUser("nfs-gateway"), configuration, () -> client);

    NfsStagingStore.StagedRange first = store.stage(EXPORT_UUID, TRANSFER_ID, 3, data);
    NfsStagingStore.StagedRange retry = store.stage(EXPORT_UUID, TRANSFER_ID, 3, data);

    assertTrue(first.getQualifiedKey().startsWith("nfs-system/staging/" + EXPORT_UUID + "/" + TRANSFER_ID
        + "/00000000000000000003/"));
    assertEquals(first.getQualifiedKey(), retry.getQualifiedKey());
    assertEquals(data.length, first.getLength());
    assertEquals(first.getSha256(), retry.getSha256());
    verify(output).write(data);
    verify(output).close();
    verify(client, org.mockito.Mockito.times(2)).createFile(eq("nfs-system"), eq("staging"), anyString(),
        eq((long) data.length), eq(RatisReplicationConfig.getInstance(ReplicationFactor.THREE.toProto())),
        eq(false), eq(true));
  }

  @Test
  void rejectsAConflictingReplay() throws Exception {
    RpcClient client = mock(RpcClient.class);
    byte[] data = "other bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    String key = EXPORT_UUID + "/" + TRANSFER_ID + "/00000000000000000000";
    OzoneBucket stagingBucket = stagingBucket();
    when(client.getBucketDetails("nfs-system", "staging")).thenReturn(stagingBucket);
    doThrow(new OMException("exists", OMException.ResultCodes.KEY_ALREADY_EXISTS))
        .when(client).createFile(eq("nfs-system"), eq("staging"), anyString(), eq((long) data.length),
            any(ReplicationConfig.class), eq(false), eq(true));
    when(client.readFile(eq("nfs-system"), eq("staging"), anyString()))
        .thenReturn(new OzoneInputStream(new ByteArrayInputStream("different".getBytes(
            java.nio.charset.StandardCharsets.UTF_8))));
    NfsStagingStore store = new NfsStagingStore("nfs-system", "staging",
        UserGroupInformation.createRemoteUser("nfs-gateway"), configuration, () -> client);

    assertThrows(OMException.class, () -> store.stage(EXPORT_UUID, TRANSFER_ID, 0, data));
  }

  @Test
  void rejectsMissingStagingBucketAndInvalidIdentity() throws Exception {
    NfsStagingStore store = new NfsStagingStore("", "staging",
        UserGroupInformation.createRemoteUser("nfs-gateway"), configuration, () -> mock(RpcClient.class));
    assertThrows(IOException.class, () -> store.stage(EXPORT_UUID, TRANSFER_ID, 0, new byte[]{1}));
    NfsStagingStore configured = new NfsStagingStore("nfs-system", "staging",
        UserGroupInformation.createRemoteUser("nfs-gateway"), configuration, () -> mock(RpcClient.class));
    assertThrows(IllegalArgumentException.class,
        () -> configured.stage("invalid", TRANSFER_ID, 0, new byte[]{1}));
  }

  @Test
  void rejectsRangeEndOverflow() {
    NfsStagingStore store = new NfsStagingStore("nfs-system", "staging",
        UserGroupInformation.createRemoteUser("nfs-gateway"), configuration, () -> mock(RpcClient.class));
    assertThrows(IOException.class, () -> store.stage(EXPORT_UUID, TRANSFER_ID, Long.MAX_VALUE,
        new byte[] {1}));
  }

  @Test
  void rejectsEcStagingReplication() throws Exception {
    RpcClient client = mock(RpcClient.class);
    OzoneBucket stagingBucket = stagingBucket();
    when(stagingBucket.getReplicationConfig()).thenReturn(new ECReplicationConfig(3, 2));
    when(client.getBucketDetails("nfs-system", "staging")).thenReturn(stagingBucket);
    NfsStagingStore store = new NfsStagingStore("nfs-system", "staging",
        UserGroupInformation.createRemoteUser("nfs-gateway"), configuration, () -> client);

    assertThrows(IOException.class, () -> store.stage(EXPORT_UUID, TRANSFER_ID, 0, new byte[] {1}));
    verify(client, never()).createFile(eq("nfs-system"), eq("staging"), anyString(), eq(1L),
        any(ReplicationConfig.class), eq(false), eq(true));
  }

  private static OzoneBucket stagingBucket() {
    OzoneBucket bucket = mock(OzoneBucket.class);
    when(bucket.getBucketLayout()).thenReturn(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    when(bucket.getReplicationConfig()).thenReturn(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE.toProto()));
    return bucket;
  }
}
