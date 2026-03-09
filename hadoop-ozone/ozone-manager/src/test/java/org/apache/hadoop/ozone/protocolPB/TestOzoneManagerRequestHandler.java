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

package org.apache.hadoop.ozone.protocolPB;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BasicOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.ListKeysLightResult;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.util.Time;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Test class to test out OzoneManagerRequestHandler.
 */
public class TestOzoneManagerRequestHandler {

  private OzoneManagerRequestHandler getRequestHandler(int limitListKeySize) {
    OmConfig config = OzoneConfiguration.newInstanceOf(OmConfig.class);
    config.setMaxListSize(limitListKeySize);
    OzoneManager ozoneManager = Mockito.mock(OzoneManager.class);
    Mockito.when(ozoneManager.getConfig()).thenReturn(config);
    return new OzoneManagerRequestHandler(ozoneManager);
  }

  private OmKeyInfo getMockedOmKeyInfo() {
    OmKeyInfo keyInfo = Mockito.mock(OmKeyInfo.class);
    OzoneManagerProtocolProtos.KeyInfo info =
        OzoneManagerProtocolProtos.KeyInfo.newBuilder().setBucketName("bucket").setKeyName("key").setVolumeName(
                "volume").setDataSize(0).setType(HddsProtos.ReplicationType.RATIS).setCreationTime(0)
            .setModificationTime(0).build();
    Mockito.when(keyInfo.getProtobuf(Mockito.anyBoolean(), Mockito.anyInt())).thenReturn(info);
    Mockito.when(keyInfo.getProtobuf(Mockito.anyInt())).thenReturn(info);
    return keyInfo;
  }

  private BasicOmKeyInfo getMockedBasicOmKeyInfo() {
    BasicOmKeyInfo keyInfo = Mockito.mock(BasicOmKeyInfo.class);
    Mockito.when(keyInfo.getProtobuf()).thenReturn(
        OzoneManagerProtocolProtos.BasicKeyInfo.newBuilder().setKeyName("key").setDataSize(0)
            .setType(HddsProtos.ReplicationType.RATIS).setCreationTime(0).setModificationTime(0)
            .build());
    return keyInfo;
  }

  private OzoneFileStatus getMockedOzoneFileStatus() {
    return new OzoneFileStatus(getMockedOmKeyInfo(), 256, false);
  }

  /**
   * Create OmKeyInfo object with or without FileEncryptionInfo.
   */
  private OmKeyInfo createOmKeyInfoWithEncryption(String keyName, boolean isEncrypted) {
    OmKeyInfo.Builder builder = new OmKeyInfo.Builder()
        .setVolumeName("testVolume")
        .setBucketName("testBucket")
        .setKeyName(keyName)
        .setDataSize(1024L)
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setReplicationConfig(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE))
        .setOmKeyLocationInfos(Collections.singletonList(new OmKeyLocationInfoGroup(0, Collections.emptyList())))
        .setAcls(Collections.emptyList())
        .setObjectID(1L)
        .setUpdateID(1L)
        .setOwnerName("testOwner");

    if (isEncrypted) {
      FileEncryptionInfo fileEncryptionInfo = new FileEncryptionInfo(
          CipherSuite.AES_CTR_NOPADDING,
          CryptoProtocolVersion.ENCRYPTION_ZONES,
          new byte[32],
          new byte[16],
          "testkey1",
          "testkey1@0"
      );
      builder.setFileEncryptionInfo(fileEncryptionInfo);
    }

    return builder.build();
  }

  private void mockOmRequest(OzoneManagerProtocolProtos.OMRequest request,
                             OzoneManagerProtocolProtos.Type cmdType,
                             int requestSize) {
    Mockito.when(request.getTraceID()).thenReturn("traceId");
    Mockito.when(request.getCmdType()).thenReturn(cmdType);
    switch (cmdType) {
    case ListKeysLight:
    case ListKeys:
      Mockito.when(request.getListKeysRequest()).thenReturn(OzoneManagerProtocolProtos.ListKeysRequest.newBuilder()
          .setCount(requestSize).setBucketName("bucket").setVolumeName("volume").setPrefix("").setStartKey("")
          .build());
      break;
    case ListStatus:
      Mockito.when(request.getListStatusRequest()).thenReturn(OzoneManagerProtocolProtos.ListStatusRequest.newBuilder()
          .setNumEntries(requestSize).setKeyArgs(OzoneManagerProtocolProtos.KeyArgs.newBuilder().setBucketName(
                  "bucket").setVolumeName("volume").setKeyName("keyName")
              .setLatestVersionLocation(true).setHeadOp(true)).setRecursive(true).setStartKey("")
          .build());
      break;
    default:
      break;
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 9, 10, 11, 50})
  public void testListKeysResponseSize(int resultSize) throws IOException {
    List<OmKeyInfo> keyInfos = IntStream.range(0, resultSize).mapToObj(i -> getMockedOmKeyInfo()).collect(
        Collectors.toList());
    OzoneManagerRequestHandler requestHandler = getRequestHandler(10);
    OzoneManager ozoneManager = requestHandler.getOzoneManager();
    Mockito.when(ozoneManager.listKeys(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
        Mockito.anyString(), Mockito.anyInt())).thenAnswer(i -> {
          int maxSize = Math.max(Math.min(resultSize, i.getArgument(4)), 0);
          return new ListKeysResult(keyInfos.isEmpty() ? keyInfos : keyInfos.subList(0, maxSize),
              maxSize < resultSize);
        });
    OzoneManagerProtocolProtos.OMRequest request = Mockito.mock(OzoneManagerProtocolProtos.OMRequest.class);
    for (int requestSize : Arrays.asList(0, resultSize - 1, resultSize, resultSize + 1, Integer.MAX_VALUE)) {
      mockOmRequest(request, OzoneManagerProtocolProtos.Type.ListKeys, requestSize);
      OzoneManagerProtocolProtos.OMResponse omResponse = requestHandler.handleReadRequest(request);
      int expectedSize = Math.max(Math.min(Math.min(10, requestSize), resultSize), 0);
      Assertions.assertEquals(expectedSize, omResponse.getListKeysResponse().getKeyInfoList().size());
      Assertions.assertEquals(expectedSize < resultSize, omResponse.getListKeysResponse().getIsTruncated());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 9, 10, 11, 50})
  public void testListLightKeysResponseSize(int resultSize) throws IOException {
    List<BasicOmKeyInfo> keyInfos = IntStream.range(0, resultSize).mapToObj(i -> getMockedBasicOmKeyInfo()).collect(
        Collectors.toList());
    OzoneManagerRequestHandler requestHandler = getRequestHandler(10);
    OzoneManager ozoneManager = requestHandler.getOzoneManager();
    Mockito.when(ozoneManager.listKeysLight(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
        Mockito.anyString(), Mockito.anyInt())).thenAnswer(i -> {
          int maxSize = Math.max(Math.min(resultSize, i.getArgument(4)), 0);
          return new ListKeysLightResult(keyInfos.isEmpty() ? keyInfos : keyInfos.subList(0, maxSize),
              maxSize < resultSize);
        });
    OzoneManagerProtocolProtos.OMRequest request = Mockito.mock(OzoneManagerProtocolProtos.OMRequest.class);
    for (int requestSize : Arrays.asList(0, resultSize - 1, resultSize, resultSize + 1, Integer.MAX_VALUE)) {
      mockOmRequest(request, OzoneManagerProtocolProtos.Type.ListKeysLight, requestSize);
      OzoneManagerProtocolProtos.OMResponse omResponse = requestHandler.handleReadRequest(request);
      int expectedSize = Math.max(Math.min(Math.min(10, requestSize), resultSize), 0);
      Assertions.assertEquals(expectedSize, omResponse.getListKeysLightResponse().getBasicKeyInfoList().size());
      Assertions.assertEquals(expectedSize < resultSize,
          omResponse.getListKeysLightResponse().getIsTruncated());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 9, 10, 11, 50})
  public void testListStatusResponseSize(int resultSize) throws IOException {
    List<OzoneFileStatus> statusList = IntStream.range(0, resultSize).mapToObj(i -> getMockedOzoneFileStatus())
        .collect(Collectors.toList());
    OzoneManagerRequestHandler requestHandler = getRequestHandler(10);
    OzoneManager ozoneManager = requestHandler.getOzoneManager();
    Mockito.when(ozoneManager.listStatus(Mockito.any(OmKeyArgs.class), Mockito.anyBoolean(), Mockito.anyString(),
        Mockito.anyLong(), Mockito.anyBoolean())).thenAnswer(i -> {
          long maxSize = i.getArgument(3);
          maxSize = Math.max(Math.min(resultSize, maxSize), 0);
          return statusList.isEmpty() ? statusList : statusList.subList(0, (int) maxSize);
        });
    OzoneManagerProtocolProtos.OMRequest request = Mockito.mock(OzoneManagerProtocolProtos.OMRequest.class);
    for (int requestSize : Arrays.asList(0, resultSize - 1, resultSize, resultSize + 1, Integer.MAX_VALUE)) {
      mockOmRequest(request, OzoneManagerProtocolProtos.Type.ListStatus, requestSize);
      OzoneManagerProtocolProtos.OMResponse omResponse = requestHandler.handleReadRequest(request);
      int expectedSize = Math.max(Math.min(Math.min(10, requestSize), resultSize), 0);
      Assertions.assertEquals(expectedSize, omResponse.getListStatusResponse().getStatusesList().size());
    }
  }

  /**
   * Test to verify BasicOmKeyInfo encryption field works in listKeysLight.
   */
  @Test
  public void testListKeysLightEncryptionFromOmKeyInfo() throws IOException {
    // Create OmKeyInfo objects with and without FileEncryptionInfo
    OmKeyInfo encryptedOmKeyInfo = createOmKeyInfoWithEncryption("encrypted-key", true);
    OmKeyInfo normalOmKeyInfo = createOmKeyInfoWithEncryption("normal-key", false);

    // Convert to BasicOmKeyInfo
    BasicOmKeyInfo encryptedBasicKey = BasicOmKeyInfo.fromOmKeyInfo(encryptedOmKeyInfo);
    BasicOmKeyInfo normalBasicKey = BasicOmKeyInfo.fromOmKeyInfo(normalOmKeyInfo);

    Assertions.assertTrue(encryptedBasicKey.isEncrypted());
    Assertions.assertFalse(normalBasicKey.isEncrypted());

    List<BasicOmKeyInfo> keyInfos = Arrays.asList(encryptedBasicKey, normalBasicKey);
    OzoneManagerRequestHandler requestHandler = getRequestHandler(10);
    OzoneManager ozoneManager = requestHandler.getOzoneManager();
    Mockito.when(ozoneManager.listKeysLight(Mockito.anyString(), Mockito.anyString(),
        Mockito.anyString(), Mockito.anyString(), Mockito.anyInt()))
        .thenReturn(new ListKeysLightResult(keyInfos, false));
    OzoneManagerProtocolProtos.OMRequest request = Mockito.mock(OzoneManagerProtocolProtos.OMRequest.class);
    mockOmRequest(request, OzoneManagerProtocolProtos.Type.ListKeysLight, 10);
    OzoneManagerProtocolProtos.OMResponse omResponse = requestHandler.handleReadRequest(request);

    List<OzoneManagerProtocolProtos.BasicKeyInfo> basicKeyInfoList =
        omResponse.getListKeysLightResponse().getBasicKeyInfoList();

    Assertions.assertEquals(2, basicKeyInfoList.size());
    Assertions.assertTrue(basicKeyInfoList.get(0).getIsEncrypted(), "encrypted-key should have isEncrypted=true");
    Assertions.assertFalse(basicKeyInfoList.get(1).getIsEncrypted(), "normal-key should have isEncrypted=false");
  }

  /**
   * Verify that when handleWriteRequestImpl encounters an exception during
   * validateAndUpdateCache, the audit logger is called with the correct
   * Transaction index and Command type in the audit message.
   */
  @Test
  public void testWriteRequestExceptionAuditLog() {
    OzoneManagerRequestHandler handler = getRequestHandler(10);
    OzoneManager ozoneManager = handler.getOzoneManager();

    // Set up audit logger mock
    AuditLogger auditLogger = Mockito.mock(AuditLogger.class);
    Mockito.when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);

    // Set up perf metrics (needed by captureLatencyNs)
    OMPerformanceMetrics perfMetrics = Mockito.mock(OMPerformanceMetrics.class);
    Mockito.when(perfMetrics.getValidateAndUpdateCacheLatencyNs())
        .thenReturn(Mockito.mock(MutableRate.class));
    Mockito.when(ozoneManager.getPerfMetrics()).thenReturn(perfMetrics);

    // Disable ACLs so preExecute doesn't need ACL setup
    Mockito.when(ozoneManager.getAclsEnabled()).thenReturn(false);

    // Leave getMetrics() returning null -> NPE in validateAndUpdateCache
    // when OMVolumeCreateRequest calls ozoneManager.getMetrics().incNumVolumeCreates()

    // Build a CreateVolume OMRequest
    OzoneManagerProtocolProtos.VolumeInfo volInfo =
        OzoneManagerProtocolProtos.VolumeInfo.newBuilder()
            .setAdminName("admin").setOwnerName("owner")
            .setVolume("testVol").build();
    OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCreateVolumeRequest(
                OzoneManagerProtocolProtos.CreateVolumeRequest.newBuilder()
                    .setVolumeInfo(volInfo))
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateVolume)
            .setClientId("test-client")
            .setUserInfo(OzoneManagerProtocolProtos.UserInfo.newBuilder()
                .setUserName("testUser").setHostName("localhost")
                .setRemoteAddress("127.0.0.1"))
            .build();

    ExecutionContext context = ExecutionContext.of(10, TermIndex.valueOf(1, 10));

    // handleWriteRequestImpl should throw (NPE from null getMetrics())
    // but the catch block logs the audit message first
    Assertions.assertThrows(NullPointerException.class,
        () -> handler.handleWriteRequestImpl(omRequest, context));

    // Verify auditLogger.logWrite was called with correct content
    ArgumentCaptor<AuditMessage> captor =
        ArgumentCaptor.forClass(AuditMessage.class);
    Mockito.verify(auditLogger).logWrite(captor.capture());

    String formatted = captor.getValue().getFormattedMessage();
    Assertions.assertTrue(formatted.contains("Transaction") && formatted.contains("10"),
        "Audit message should contain Transaction 10, got: " + formatted);
    Assertions.assertTrue(formatted.contains("Command") && formatted.contains("CreateVolume"),
        "Audit message should contain Command CreateVolume, got: " + formatted);
  }
}
