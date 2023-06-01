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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.client.rpc;

import java.io.IOException;

import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerClientProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.ClientId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test cases for RpcClientFileLease.
 */
public class TestRpcClientFileLease {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestRpcClientFileLease.class);

  private static final long FAST_GRACE_PERIOD = 100L;

  @Test
  public void testKeyId() throws Exception {
    String vol1 = "vol1";
    String bucket1 = "bucket1";
    String f1 = "f1";

    String vol2 = "vol2";
    String bucket2 = "bucket2";
    String f2 = "f2";
    OmKeyInfo keyInfo =  new OmKeyInfo.Builder()
        .setVolumeName(vol1)
        .setBucketName(bucket1)
        .setKeyName(f1).build();
    RpcClientFileLease.KeyIdentifier keyId =
        new RpcClientFileLease.KeyIdentifier(keyInfo);
    OmKeyInfo keyInfo2 =  new OmKeyInfo.Builder()
        .setVolumeName(vol2)
        .setBucketName(bucket2)
        .setKeyName(f2).build();
    RpcClientFileLease.KeyIdentifier keyId2 =
        new RpcClientFileLease.KeyIdentifier(keyInfo2);

    assertNotEquals(keyId, keyId2);
  }

  @Test
  public void testLease() throws IOException {
    String vol1 = "vol1";
    String bucket1 = "bucket1";
    String f1 = "f1";

    String vol2 = "vol2";
    String bucket2 = "bucket2";
    String f2 = "f2";
    OmKeyInfo keyInfo =  new OmKeyInfo.Builder()
        .setVolumeName(vol1)
        .setBucketName(bucket1)
        .setKeyName(f1).build();
    OmKeyInfo keyInfo2 =  new OmKeyInfo.Builder()
        .setVolumeName(vol2)
        .setBucketName(bucket2)
        .setKeyName(f2).build();
    RpcClientFileLease.KeyIdentifier keyId =
        new RpcClientFileLease.KeyIdentifier(keyInfo);
    final RpcClientFileLease.KeyIdentifier keyId2 =
        new RpcClientFileLease.KeyIdentifier(keyInfo2);
    KeyOutputStream outputStream = Mockito.mock(KeyOutputStream.class);

    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        "user1", new String[] {"group1"});

    OzoneManagerClientProtocol client =
        Mockito.mock(OzoneManagerClientProtocol.class);
    ClientId clientId = ClientId.randomId();
    OzoneClientConfig clientConfig = Mockito.mock(OzoneClientConfig.class);
    when(clientConfig.getLeaseHardLimitPeriod()).thenReturn(1000);

    RpcClient mockRpcClient = createMockClient();

    final RpcClientFileLease lease = new RpcClientFileLease(
        "ozone1", ugi, client, clientId, clientConfig, mockRpcClient);

    lease.beginFileLease(keyId, outputStream);
    assertEquals(1, lease.numFilesBeingWritten());
    // non-existent key id should throw exception.
    assertThrows(IOException.class,
        () -> lease.endFileLease(keyId2));
    lease.endFileLease(keyId);
    assertEquals(0, lease.numFilesBeingWritten());


    lease.beginFileLease(keyId, outputStream);
    lease.closeAllFilesBeingWritten(true);
    assertEquals(0, lease.numFilesBeingWritten());
    verify(outputStream, times(1)).abort();
  }

  private RpcClient createMockClient() {
    final OzoneClientConfig mockConf = Mockito.mock(OzoneClientConfig.class);
    when(mockConf.getTimeout()).thenReturn((int)FAST_GRACE_PERIOD);

    RpcClient mock = Mockito.mock(RpcClient.class);
    when(mock.isClientRunning()).thenReturn(true);
    when(mock.getConf()).thenReturn(mockConf);

    ClientId clientId = ClientId.randomId();
    when(mock.getClientId()).thenReturn(clientId);
    return mock;
  }
}
