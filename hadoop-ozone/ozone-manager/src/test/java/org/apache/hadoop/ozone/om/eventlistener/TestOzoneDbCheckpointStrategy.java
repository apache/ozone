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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
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
 * Tests {@link OzoneDbCheckpointStrategy}.
 */
@ExtendWith(MockitoExtension.class)
public class TestOzoneDbCheckpointStrategy {

  @Mock
  private OzoneManager mockOzoneManager;
  @Mock
  private OMMetadataManager mockMetadataManager;
  @Mock
  @SuppressWarnings("rawtypes")
  private Table mockMetaTable;
  @Mock
  private OzoneManagerProtocolProtos.OMResponse mockOmResponse;

  private OzoneDbCheckpointStrategy ozoneDbCheckpointStrategy;

  @BeforeEach
  public void setup() throws Exception {
    ozoneDbCheckpointStrategy = new OzoneDbCheckpointStrategy(mockOzoneManager,
        new OzoneConfiguration());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLoadStrategy() throws IOException {
    when(mockOzoneManager.getMetadataManager()).thenReturn(mockMetadataManager);
    when(mockMetadataManager.getMetaTable()).thenReturn(mockMetaTable);
    String dbKey = OzoneConsts.EVENT_NOTIFICATION_CHECKPOINT_PREFIX + "kafka-completed-ops";
    when(mockMetaTable.get(dbKey)).thenReturn("00000000000000000017");
    Assertions.assertEquals("00000000000000000017", ozoneDbCheckpointStrategy.load());
  }

  @Test
  public void testSaveStrategy() throws IOException, ServiceException {
    try (MockedStatic<OzoneManagerRatisUtils> utils = mockStatic(OzoneManagerRatisUtils.class)) {
      utils.when(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class))).thenReturn(mockOmResponse);
      when(mockOmResponse.getStatus()).thenReturn(OzoneManagerProtocolProtos.Status.OK);

      // Check its saved on first iteration
      ozoneDbCheckpointStrategy.save("00000000000000000001");
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(1));

      // Iterations 2 to 100 are throttled
      for (int i = 2; i <= 100; i++) {
        ozoneDbCheckpointStrategy.save(String.valueOf(i));
      }
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(1));

      // Iteration 101 should write immediately!
      ozoneDbCheckpointStrategy.save("00000000000000000101");
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(2));
    }
  }

  @Test
  public void testSaveStrategyBypassesThrottlingUponFailureRecovery() throws IOException, ServiceException {
    try (MockedStatic<OzoneManagerRatisUtils> utils = mockStatic(OzoneManagerRatisUtils.class)) {
      utils.when(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class))).thenReturn(mockOmResponse);

      // First save succeeds (saveCount becomes 1)
      when(mockOmResponse.getStatus()).thenReturn(OzoneManagerProtocolProtos.Status.OK);
      ozoneDbCheckpointStrategy.save("1");
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(1));

      // Saves 2 to 100 are throttled
      for (int i = 2; i <= 100; i++) {
        ozoneDbCheckpointStrategy.save(String.valueOf(i));
      }
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(1));

      // Save 101 tries to write and fails!
      when(mockOmResponse.getStatus()).thenReturn(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND);
      Assertions.assertThrows(IOException.class, () -> ozoneDbCheckpointStrategy.save("101"));
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(2));

      // Save 102 would normally be throttled, but because 101 failed,
      // it should bypass throttling and try to write immediately!
      when(mockOmResponse.getStatus()).thenReturn(OzoneManagerProtocolProtos.Status.OK);
      ozoneDbCheckpointStrategy.save("102");
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(3));
    }
  }

  @Test
  public void testExplicitReset() throws IOException, ServiceException {
    try (MockedStatic<OzoneManagerRatisUtils> utils = mockStatic(OzoneManagerRatisUtils.class)) {
      utils.when(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class))).thenReturn(mockOmResponse);
      when(mockOmResponse.getStatus()).thenReturn(OzoneManagerProtocolProtos.Status.OK);

      // Calling reset() writes empty string immediately, even if throttled count is non-zero
      ozoneDbCheckpointStrategy.reset();
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class),
          any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(1));
    }
  }
}
