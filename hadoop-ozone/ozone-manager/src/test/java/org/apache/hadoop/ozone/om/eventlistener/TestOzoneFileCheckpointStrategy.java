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

package org.apache.hadoop.ozone.om.eventlistener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import org.apache.hadoop.ozone.om.IOmMetadataReader;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.protocol.ClientId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests {@link OzoneFileCheckpointStrategy}.
 */
@ExtendWith(MockitoExtension.class)
public class TestOzoneFileCheckpointStrategy {

  @Mock
  private OzoneManager mockOzoneManager;
  @Mock
  private IOmMetadataReader mockOmMetadataReader;
  @Mock
  private OzoneManagerProtocolProtos.OMResponse mockOmResponse;
  @Mock
  private OmBucketInfo mockBucketInfo;

  private OzoneFileCheckpointStrategy ozoneFileCheckpointStrategy;

  @BeforeEach
  public void setup() {
    ozoneFileCheckpointStrategy = new OzoneFileCheckpointStrategy(mockOzoneManager, mockOmMetadataReader,
        new org.apache.hadoop.hdds.conf.OzoneConfiguration());
  }

  @Test
  public void testSaveStrategy() throws IOException, ServiceException {
    try (MockedStatic<OzoneManagerRatisUtils> utils = mockStatic(OzoneManagerRatisUtils.class)) {
      utils.when(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class))).thenReturn(mockOmResponse);
      when(mockOmResponse.getStatus()).thenReturn(OzoneManagerProtocolProtos.Status.OK);

      // Check its saved on first iteration
      ozoneFileCheckpointStrategy.save("00000000000000000001");
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(1));

      // But not on second
      ozoneFileCheckpointStrategy.save("0000000000000000002");
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(1));

      for (int i = 0; i <= 100; i++) {
        String val = String.format("%020d", i);
        ozoneFileCheckpointStrategy.save(val);
      }

      // Check submit has only ran twice in total (first save + save at 100th iteration)
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(2));
    }
  }

  @Test
  public void testLoadStrategyWhenMetadataNotSet() throws IOException {
    when(mockOzoneManager.getBucketInfo(any(), any())).thenReturn(mockBucketInfo);
    when(mockBucketInfo.getMetadata()).thenReturn(com.google.common.collect.ImmutableMap.of());
    Assertions.assertNull(ozoneFileCheckpointStrategy.load());
  }

  @Test
  public void testLoadStrategyWhenBucketDoesNotExist() throws IOException {
    when(mockOzoneManager.getBucketInfo(any(), any())).thenThrow(IOException.class);
    Assertions.assertThrows(IOException.class, () -> ozoneFileCheckpointStrategy.load());
  }

  @Test
  public void testLoadStrategyWithValidMetaData() throws IOException {
    when(mockOzoneManager.getBucketInfo(any(), any())).thenReturn(mockBucketInfo);
    when(mockBucketInfo.getMetadata()).thenReturn(
        com.google.common.collect.ImmutableMap.of("notification-checkpoint", "00000000000000000017"));
    Assertions.assertEquals("00000000000000000017", ozoneFileCheckpointStrategy.load());
  }

  @Test
  public void testSaveStrategyBypassesThrottlingUponFailureRecovery() throws IOException, ServiceException {
    try (MockedStatic<OzoneManagerRatisUtils> utils = mockStatic(OzoneManagerRatisUtils.class)) {
      utils.when(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class))).thenReturn(mockOmResponse);

      // First save succeeds (saveCount becomes 1)
      when(mockOmResponse.getStatus()).thenReturn(OzoneManagerProtocolProtos.Status.OK);
      ozoneFileCheckpointStrategy.save("1");
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(1));

      // Saves 2 to 100 are throttled
      for (int i = 2; i <= 100; i++) {
        ozoneFileCheckpointStrategy.save(String.valueOf(i));
      }
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(1));

      // Save 101 tries to write and fails!
      when(mockOmResponse.getStatus()).thenReturn(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND);
      Assertions.assertThrows(IOException.class, () -> ozoneFileCheckpointStrategy.save("101"));
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(2));

      // Save 102 would normally be throttled, but because 101 failed,
      // it should bypass throttling and try to write immediately!
      when(mockOmResponse.getStatus()).thenReturn(OzoneManagerProtocolProtos.Status.OK);
      ozoneFileCheckpointStrategy.save("102");
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(3));

      // Save 103 is throttled again (throttling restored)
      ozoneFileCheckpointStrategy.save("103");
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(3));
    }
  }
}
