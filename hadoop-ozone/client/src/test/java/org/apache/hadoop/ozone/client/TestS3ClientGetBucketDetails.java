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

package org.apache.hadoop.ozone.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3VolumeContextResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Verifies that the S3 volume of {@code getBucketDetails(null, bucket)} is resolved on OM in the InfoBucket call
 * (HDDS-16681), and that an S3 client falls back to a separate GetS3VolumeContext call for an older OM.
 */
public class TestS3ClientGetBucketDetails {

  private static final String TENANT_VOLUME = "tenantvol";
  private static final String BUCKET = "bucket1";
  private static final String ACCESS_ID = "tenant$user";
  private static final String USER_PRINCIPAL = "user";

  private final List<OMRequest> requests = new ArrayList<>();

  /**
   * Creates a client against a mock OM reporting {@code omVersion}, wrapped in {@link S3ClientProtocol} like S3 Gateway
   * clients if {@code s3Client}. The mock OM maps every S3 request to {@link #TENANT_VOLUME}, like OM does for a tenant
   * user, but only honors assumeS3Context in InfoBucket when {@code omVersion} supports it.
   */
  private OzoneClient newClient(OzoneManagerVersion omVersion, boolean s3Client) throws IOException {
    InMemoryConfigurationForTesting conf = new InMemoryConfigurationForTesting();
    conf.setFromObject(conf.getObject(OzoneClientConfig.class));
    boolean resolvesS3Context = OzoneManagerVersion.S3_VOLUME_CONTEXT_RESOLUTION.isSupportedBy(omVersion);
    RpcClient rpcClient = new RpcClient(conf, null) {
      @Override
      protected OmTransport createOmTransport(String omServiceId) {
        return new MockOmTransport() {
          @Override
          public OMResponse submitRequest(OMRequest payload) throws IOException {
            requests.add(payload);
            switch (payload.getCmdType()) {
            case ServiceList:
              return OMResponse.newBuilder()
                  .setCmdType(Type.ServiceList)
                  .setStatus(Status.OK)
                  .setServiceListResponse(ServiceListResponse.newBuilder()
                      .addServiceInfo(ServiceInfo.newBuilder()
                          .setNodeType(NodeType.OM)
                          .setHostname("om")
                          .setOMVersion(omVersion.serialize())))
                  .build();
            case GetS3VolumeContext:
              OMResponse volumeResponse = super.submitRequest(OMRequest.newBuilder(payload)
                  .setCmdType(Type.InfoVolume)
                  .setInfoVolumeRequest(InfoVolumeRequest.newBuilder().setVolumeName(TENANT_VOLUME))
                  .build());
              return OMResponse.newBuilder()
                  .setCmdType(Type.GetS3VolumeContext)
                  .setStatus(Status.OK)
                  .setGetS3VolumeContextResponse(GetS3VolumeContextResponse.newBuilder()
                      .setVolumeInfo(volumeResponse.getInfoVolumeResponse().getVolumeInfo())
                      .setUserPrincipal(USER_PRINCIPAL))
                  .build();
            case InfoBucket:
              if (resolvesS3Context && payload.getAssumeS3Context()) {
                OMResponse bucketResponse = super.submitRequest(OMRequest.newBuilder(payload)
                    .setInfoBucketRequest(InfoBucketRequest.newBuilder(payload.getInfoBucketRequest())
                        .setVolumeName(TENANT_VOLUME))
                    .build());
                return OMResponse.newBuilder(bucketResponse).setUserPrincipal(USER_PRINCIPAL).build();
              }
              return super.submitRequest(payload);
            default:
              return super.submitRequest(payload);
            }
          }
        };
      }

      @Nonnull
      @Override
      protected XceiverClientFactory createXceiverClientFactory(ServiceInfoEx serviceInfo) {
        return new MockXceiverClientFactory();
      }
    };
    ClientProtocol proxy = s3Client ? new S3ClientProtocol(rpcClient) : rpcClient;
    OzoneClient client = new OzoneClient(conf, proxy);
    client.getObjectStore().createVolume(TENANT_VOLUME);
    client.getObjectStore().getVolume(TENANT_VOLUME).createBucket(BUCKET);
    requests.clear();
    proxy.setThreadLocalS3Auth(new S3Auth("unused1", "unused2", ACCESS_ID, ACCESS_ID));
    return client;
  }

  @ParameterizedTest
  @CsvSource({"S3_VOLUME_CONTEXT_RESOLUTION, true", "GET_KEY_INFO_REJECTS_OBS, true",
      "S3_VOLUME_CONTEXT_RESOLUTION, false"})
  public void resolvesS3VolumeInInfoBucket(OzoneManagerVersion omVersion, boolean s3Client) throws IOException {
    try (OzoneClient client = newClient(omVersion, s3Client)) {
      OzoneBucket bucket = client.getProxy().getBucketDetails(null, BUCKET);

      assertEquals(TENANT_VOLUME, bucket.getVolumeName());
      assertEquals(BUCKET, bucket.getName());
      assertThat(requests).extracting(OMRequest::getCmdType).containsExactly(Type.InfoBucket);
      assertTrue(requests.get(0).getAssumeS3Context());
      assertEquals(USER_PRINCIPAL, client.getProxy().getThreadLocalS3Auth().getUserPrincipal());
    }
  }

  @ParameterizedTest
  @EnumSource(value = OzoneManagerVersion.class,
      names = {"GET_FILE_STATUS_REJECTS_OBS", "ZDU", "LIST_STATUS_REJECTS_OBS"})
  public void fallsBackToS3VolumeContextForOlderOm(OzoneManagerVersion omVersion) throws IOException {
    try (OzoneClient client = newClient(omVersion, true)) {
      OzoneBucket bucket = client.getProxy().getBucketDetails(null, BUCKET);

      assertEquals(TENANT_VOLUME, bucket.getVolumeName());
      assertEquals(BUCKET, bucket.getName());
      assertThat(requests).extracting(OMRequest::getCmdType)
          .containsExactly(Type.GetS3VolumeContext, Type.InfoBucket);
      // An older OM ignores assumeS3Context, so the resolved volume must be sent instead.
      assertEquals(TENANT_VOLUME, requests.get(1).getInfoBucketRequest().getVolumeName());
      assertEquals(USER_PRINCIPAL, client.getProxy().getThreadLocalS3Auth().getUserPrincipal());
    }
  }

  @ParameterizedTest
  @EnumSource(value = OzoneManagerVersion.class, names = {"ZDU", "S3_VOLUME_CONTEXT_RESOLUTION"})
  public void preservesExplicitVolume(OzoneManagerVersion omVersion) throws IOException {
    try (OzoneClient client = newClient(omVersion, true)) {
      OzoneBucket bucket = client.getProxy().getBucketDetails(TENANT_VOLUME, BUCKET);

      assertEquals(TENANT_VOLUME, bucket.getVolumeName());
      assertThat(requests).extracting(OMRequest::getCmdType).containsExactly(Type.InfoBucket);
      assertFalse(requests.get(0).getAssumeS3Context());
      assertEquals(TENANT_VOLUME, requests.get(0).getInfoBucketRequest().getVolumeName());
    }
  }
}
