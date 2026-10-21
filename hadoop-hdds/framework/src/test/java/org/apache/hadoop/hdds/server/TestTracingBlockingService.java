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

package org.apache.hadoop.hdds.server;

import static org.apache.hadoop.hdds.tracing.OzoneContextStorageProvider.MDC_TRACE_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.ServiceException;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.protocol.ReconfigureProtocol;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.GetServerNameRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.ReconfigureProtocolService;
import org.apache.hadoop.hdds.protocolPB.ReconfigureProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.tracing.TestSpanCollector;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

/**
 * Tests for {@link TracingBlockingService}.
 */
public class TestTracingBlockingService {

  private static final String SPAN_NAME = "ReconfigureProtocolService.getServerName";

  @Test
  public void testTracing() throws Exception {
    ReconfigureProtocol impl = mock(ReconfigureProtocol.class);
    AtomicReference<String> traceIdInCall = new AtomicReference<>();
    AtomicReference<String> mdcTraceIdInCall = new AtomicReference<>();
    when(impl.getServerName()).thenAnswer(invocation -> {
      traceIdInCall.set(Span.current().getSpanContext().getTraceId());
      mdcTraceIdInCall.set(MDC.get(MDC_TRACE_ID));
      return "om";
    });

    try (TestSpanCollector spans = TestSpanCollector.install("TestTracingBlockingService")) {
      callGetServerName(impl);

      assertThat(spans.named(SPAN_NAME)).hasSize(1).allSatisfy(span -> {
        assertThat(span.getKind()).isEqualTo(SpanKind.SERVER);
        assertThat(span.getSpanContext().getTraceId()).isEqualTo(traceIdInCall.get());
      });
      assertThat(mdcTraceIdInCall.get()).isEqualTo(traceIdInCall.get());
    }
  }

  @Test
  public void testTracingRecordsFailure() throws Exception {
    ReconfigureProtocol impl = mock(ReconfigureProtocol.class);
    when(impl.getServerName()).thenThrow(new IOException("failed"));

    try (TestSpanCollector spans = TestSpanCollector.install("TestTracingBlockingService")) {
      assertThatThrownBy(() -> callGetServerName(impl)).isInstanceOf(ServiceException.class);

      assertThat(spans.named(SPAN_NAME)).hasSize(1)
          .allSatisfy(span -> assertThat(span.toSpanData().getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR));
    }
  }

  private static void callGetServerName(ReconfigureProtocol impl) throws ServiceException {
    BlockingService service = TracingBlockingService.wrap(ReconfigureProtocolService.newReflectiveBlockingService(
        new ReconfigureProtocolServerSideTranslatorPB(impl)));
    MethodDescriptor method = service.getDescriptorForType().findMethodByName("getServerName");
    service.callBlockingMethod(method, null, GetServerNameRequestProto.getDefaultInstance());
  }
}
