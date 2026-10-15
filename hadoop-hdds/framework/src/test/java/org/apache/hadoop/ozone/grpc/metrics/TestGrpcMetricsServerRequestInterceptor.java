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

package org.apache.hadoop.ozone.grpc.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.SCMBlockLocationRequest;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.Type;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.UserInfo;
import org.apache.hadoop.metrics2.lib.MetricsAnnotations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link GrpcMetricsServerRequestInterceptor}.
 *
 * <p>{@code SCMBlockLocationRequest} stands in for {@code OMRequest}, which this module cannot depend on: both have
 * {@code cmdType} as field 1 and a free-text {@code traceID} as field 2.
 */
public class TestGrpcMetricsServerRequestInterceptor {

  private GrpcMetrics metrics;
  private ServerCall.Listener<Object> delegate;
  private ServerCall.Listener<Object> listener;

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setUp() {
    metrics = new GrpcMetrics(new OzoneConfiguration());
    // populates the @Metric counters, as DefaultMetricsSystem.register() does
    MetricsAnnotations.makeSource(metrics);

    delegate = mock(ServerCall.Listener.class);
    ServerCallHandler<Object, Object> handler = mock(ServerCallHandler.class);
    when(handler.startCall(any(), any())).thenReturn(delegate);
    listener = new GrpcMetricsServerRequestInterceptor(metrics)
        .interceptCall(mock(ServerCall.class), new Metadata(), handler);
  }

  @Test
  public void testRequestTypeIsReadFromCmdType() {
    SCMBlockLocationRequest request = SCMBlockLocationRequest.newBuilder()
        .setCmdType(Type.GetScmInfo)
        .setTraceID("trace:with:colons" + System.lineSeparator() + "next: line")
        .build();

    listener.onMessage(request);

    assertEquals("GetScmInfo", metrics.getRequestType());
    assertEquals(request.getSerializedSize(), metrics.getReceivedBytes());
    assertEquals(0, metrics.getUnknownMessagesReceived());
    verify(delegate).onMessage(request);
  }

  @Test
  public void testMessageWithoutCmdTypeKeepsRequestType() {
    UserInfo message = UserInfo.newBuilder().setUserName("user: name").build();

    listener.onMessage(message);

    assertEquals("NoRequest", metrics.getRequestType());
    assertEquals(message.getSerializedSize(), metrics.getReceivedBytes());
    verify(delegate).onMessage(message);
  }

  @Test
  public void testNonProtobufMessageIsCountedAsUnknown() {
    // the previous text parsing threw ArrayIndexOutOfBoundsException for a message without ':' in its first line
    String message = "not a protobuf message";

    listener.onMessage(message);

    assertEquals("NoRequest", metrics.getRequestType());
    assertEquals(1, metrics.getUnknownMessagesReceived());
    assertEquals(0, metrics.getReceivedBytes());
    verify(delegate).onMessage(message);
  }
}
