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

package org.apache.hadoop.ozone.om.ratis.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ratis.protocol.ClientId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OzoneManagerRatisUtils#submitRequest}, in particular that
 * internally-submitted requests populate the OmClientProtocol per-type metrics.
 */
public class TestOzoneManagerRatisUtils {

  private OzoneManager ozoneManager;
  private OzoneManagerRatisServer ratisServer;
  private ProtocolMessageMetrics<Type> metrics;

  @BeforeEach
  public void setup() {
    ozoneManager = mock(OzoneManager.class);
    ratisServer = mock(OzoneManagerRatisServer.class);
    metrics = ProtocolMessageMetrics.create(
        "OmClientProtocol", "Ozone Manager RPC endpoint", Type.class);
    when(ozoneManager.getOmClientProtocolMetrics()).thenReturn(metrics);
    when(ozoneManager.getOmRatisServer()).thenReturn(ratisServer);
  }

  @Test
  public void testSubmitRequestRecordsMetricForRequestType() throws Exception {
    OMRequest request = newRequest(Type.PurgeKeys);
    OMResponse expected = newResponse(Type.PurgeKeys);
    when(ratisServer.submitRequest(eq(request), any(ClientId.class), anyLong()))
        .thenReturn(expected);

    OMResponse actual = OzoneManagerRatisUtils.submitRequest(
        ozoneManager, request, ClientId.randomId(), 1L);

    assertSame(expected, actual);
    assertEquals(1, counterFor(Type.PurgeKeys));
    // Only the submitted type should be counted.
    assertEquals(0, counterFor(Type.RenameKey));
  }

  @Test
  public void testSubmitRequestIncrementsMetricPerCall() throws Exception {
    OMRequest request = newRequest(Type.PurgeKeys);
    when(ratisServer.submitRequest(any(OMRequest.class), any(ClientId.class), anyLong()))
        .thenReturn(newResponse(Type.PurgeKeys));

    OzoneManagerRatisUtils.submitRequest(ozoneManager, request, ClientId.randomId(), 1L);
    OzoneManagerRatisUtils.submitRequest(ozoneManager, request, ClientId.randomId(), 2L);

    assertEquals(2, counterFor(Type.PurgeKeys));
  }

  @Test
  public void testSubmitRequestRecordsMetricOnFailure() throws Exception {
    OMRequest request = newRequest(Type.PurgeKeys);
    when(ratisServer.submitRequest(any(OMRequest.class), any(ClientId.class), anyLong()))
        .thenThrow(new ServiceException("submit failed"));

    assertThrows(ServiceException.class, () -> OzoneManagerRatisUtils.submitRequest(
        ozoneManager, request, ClientId.randomId(), 1L));

    // The measurement wraps the submission, so the metric is recorded even when it fails.
    assertEquals(1, counterFor(Type.PurgeKeys));
  }

  private static OMRequest newRequest(Type type) {
    return OMRequest.newBuilder()
        .setCmdType(type)
        .setClientId(ClientId.randomId().toString())
        .build();
  }

  private static OMResponse newResponse(Type type) {
    return OMResponse.newBuilder()
        .setCmdType(type)
        .setStatus(Status.OK)
        .setSuccess(true)
        .build();
  }

  /**
   * Reads back the {@code counter} value recorded for the given request type
   * from the live {@link ProtocolMessageMetrics} source.
   */
  private long counterFor(Type type) {
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    metrics.getMetrics(collector, true);
    for (MetricsRecord record : collector.getRecords()) {
      boolean matchesType = false;
      for (MetricsTag tag : record.tags()) {
        if ("type".equals(tag.name()) && type.toString().equals(tag.value())) {
          matchesType = true;
          break;
        }
      }
      if (!matchesType) {
        continue;
      }
      for (AbstractMetric metric : record.metrics()) {
        if ("counter".equals(metric.name())) {
          return metric.value().longValue();
        }
      }
    }
    return 0;
  }
}
