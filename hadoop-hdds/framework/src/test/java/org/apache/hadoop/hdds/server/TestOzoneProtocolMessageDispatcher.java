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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ServiceException;
import io.opentelemetry.api.trace.SpanKind;
import org.apache.hadoop.hdds.tracing.TestSpanCollector;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * Tests the SERVER span created by {@link OzoneProtocolMessageDispatcher}.
 */
public class TestOzoneProtocolMessageDispatcher {

  private final OzoneProtocolMessageDispatcher<String, String, TestType> dispatcher =
      new OzoneProtocolMessageDispatcher<>("test", ProtocolMessageMetrics.create("test", "test", TestType.class),
          LoggerFactory.getLogger(TestOzoneProtocolMessageDispatcher.class));

  @Test
  public void testSuccessfulRequestIsNotError() throws ServiceException {
    try (TestSpanCollector spans = TestSpanCollector.install("test")) {
      assertEquals("response", dispatcher.processRequest("request", request -> "response", TestType.SUCCEED, null));
      spans.assertKind(TestType.SUCCEED.name(), SpanKind.SERVER);
      spans.assertNotError(TestType.SUCCEED.name());
    }
  }

  @Test
  public void testThrownExceptionMarksSpanError() {
    try (TestSpanCollector spans = TestSpanCollector.install("test")) {
      ServiceException ex = assertThrows(ServiceException.class, () -> dispatcher.processRequest("request",
          request -> {
            throw new ServiceException("request failed");
          }, TestType.FAIL, null));
      assertEquals("request failed", ex.getMessage());
      spans.assertKind(TestType.FAIL.name(), SpanKind.SERVER);
      spans.assertError(TestType.FAIL.name(), "request failed");
      spans.assertExceptionRecorded(TestType.FAIL.name(), ServiceException.class);
    }
  }

  @Test
  public void testRuntimeExceptionMarksSpanError() {
    try (TestSpanCollector spans = TestSpanCollector.install("test")) {
      assertThrows(IllegalStateException.class, () -> dispatcher.processRequest("request", request -> {
        throw new IllegalStateException("bug");
      }, TestType.FAIL, null));
      spans.assertError(TestType.FAIL.name(), "bug");
      spans.assertExceptionRecorded(TestType.FAIL.name(), IllegalStateException.class);
    }
  }

  private enum TestType {
    SUCCEED, FAIL
  }
}
