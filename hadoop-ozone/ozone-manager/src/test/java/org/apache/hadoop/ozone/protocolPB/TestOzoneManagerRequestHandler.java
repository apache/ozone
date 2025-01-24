package org.apache.hadoop.ozone.protocolPB;

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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BasicOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.ListKeysLightResult;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVER_LIST_MAX_SIZE;

/**
 * Test class to test out OzoneManagerRequestHandler.
 */
public class TestOzoneManagerRequestHandler {


  private OzoneManagerRequestHandler getRequestHandler(int limitListKeySize) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OZONE_OM_SERVER_LIST_MAX_SIZE, limitListKeySize);
    OzoneManager ozoneManager = Mockito.mock(OzoneManager.class);
    Mockito.when(ozoneManager.getConfiguration()).thenReturn(conf);
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
}
