/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.tracing;

import io.jaegertracing.internal.JaegerSpanContext;
import io.jaegertracing.internal.exceptions.EmptyTracerStateStringException;
import io.jaegertracing.internal.exceptions.MalformedTracerStateStringException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestStringCodec {

  @Test
  void testExtract() {
    StringCodec codec = new StringCodec();

    assertThrows(EmptyTracerStateStringException.class,
        () -> codec.extract(null));

    StringBuilder sb = new StringBuilder().append("123");
    MalformedTracerStateStringException malformedException =
        assertThrows(MalformedTracerStateStringException.class,
            () -> codec.extract(sb));
    assertEquals("String does not match tracer state format: 123",
        malformedException.getMessage());

    sb.append(":456:789");
    malformedException =
        assertThrows(MalformedTracerStateStringException.class,
            () -> codec.extract(sb));
    assertEquals("String does not match tracer state format: 123:456:789",
        malformedException.getMessage());

    sb.append(":66");
    JaegerSpanContext context = codec.extract(sb);
    StringBuilder injected = new StringBuilder();
    codec.inject(context, injected);

    String expectedTraceId = pad("123");
    assertEquals(expectedTraceId, context.getTraceId());
    assertEquals(expectedTraceId + ":456:789:66", injected.toString());
  }

  private static String pad(String s) {
    return "0000000000000000".substring(s.length()) + s;
  }
}
