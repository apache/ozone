/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.request;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.UUID;

import io.grpc.Context;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.request.key.OMKeyCommitRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketCreateRequest;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.createRequestWithS3Credentials;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.newBucketInfoBuilder;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.newCreateBucketRequest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test OMClient Request with user information.
 */
public class TestOMClientRequestWithUserInfo {

  @TempDir
  private Path folder;

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;
  private UserGroupInformation userGroupInformation =
      UserGroupInformation.createRemoteUser("temp");
  private InetAddress inetAddress;

  @BeforeEach
  public void setup() throws Exception {
    ozoneManager = Mockito.mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration,
        ozoneManager);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    inetAddress = InetAddress.getByName("127.0.0.1");
  }

  @Test
  public void testUserInfoInCaseOfHadoopTransport() throws Exception {
    new MockUp<ProtobufRpcEngine.Server>() {
      @Mock
      public UserGroupInformation getRemoteUser() {
        return userGroupInformation;
      }

      @Mock
      public InetAddress getRemoteIp() {
        return inetAddress;
      }

      public InetAddress getRemoteAddress() {
        return inetAddress;
      }
    };

    String bucketName = UUID.randomUUID().toString();
    String volumeName = UUID.randomUUID().toString();
    BucketInfo.Builder bucketInfo =
        newBucketInfoBuilder(bucketName, volumeName)
            .setIsVersionEnabled(true)
            .setStorageType(OzoneManagerProtocolProtos.StorageTypeProto.DISK);
    OMRequest omRequest = newCreateBucketRequest(bucketInfo).build();

    OMBucketCreateRequest omBucketCreateRequest =
        new OMBucketCreateRequest(omRequest);

    Assertions.assertFalse(omRequest.hasUserInfo());

    OMRequest modifiedRequest =
        omBucketCreateRequest.preExecute(ozoneManager);

    Assertions.assertTrue(modifiedRequest.hasUserInfo());

    // Now pass modified request to OMBucketCreateRequest and check ugi and
    // remote Address.
    omBucketCreateRequest = new OMBucketCreateRequest(modifiedRequest);

    InetAddress remoteAddress = omBucketCreateRequest.getRemoteAddress();
    UserGroupInformation ugi = omBucketCreateRequest.createUGI();
    String hostName = omBucketCreateRequest.getHostName();


    // Now check we have original user info, remote address and hostname or not.
    // Here from OMRequest user info, converted to UGI, InetAddress and String.
    Assertions.assertEquals(inetAddress.getHostAddress(),
        remoteAddress.getHostAddress());
    Assertions.assertEquals(userGroupInformation.getUserName(),
        ugi.getUserName());
    Assertions.assertEquals(inetAddress.getHostName(), hostName);
  }

  @Test
  public void testUserInfoInCaseOfGrpcTransport() throws IOException {
    try (MockedStatic<Context> mockedGrpcRequestContextKey =
             Mockito.mockStatic(Context.class)) {
      // given
      Context.Key<String> hostnameKey = mock(Context.Key.class);
      when(hostnameKey.get()).thenReturn("hostname");

      Context.Key<String> ipAddress = mock(Context.Key.class);
      when(ipAddress.get()).thenReturn("172.5.3.5");

      mockedGrpcRequestContextKey.when(() -> Context.key("CLIENT_HOSTNAME"))
          .thenReturn(hostnameKey);
      mockedGrpcRequestContextKey.when(() -> Context.key("CLIENT_IP_ADDRESS"))
          .thenReturn(ipAddress);

      OMRequest s3SignedOMRequest = createRequestWithS3Credentials("AccessId",
          "Signature", "StringToSign");
      OMClientRequest omClientRequest =
          new OMKeyCommitRequest(s3SignedOMRequest, mock(BucketLayout.class));

      // when
      OzoneManagerProtocolProtos.UserInfo userInfo =
          omClientRequest.getUserInfo();

      // then
      Assertions.assertEquals("hostname", userInfo.getHostName());
      Assertions.assertEquals("172.5.3.5", userInfo.getRemoteAddress());
      Assertions.assertEquals("AccessId", userInfo.getUserName());
    }
  }

}
