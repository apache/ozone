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

package org.apache.ozone.lib.wsrs;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link JSONMapProvider} JSON output framing after the json-simple to
 * Jackson migration.
 */
public class TestJSONMapProvider {

  private String writeToString(Map<String, Object> map, ByteArrayOutputStream out)
      throws IOException {
    new JSONMapProvider().writeTo(map, Map.class, Map.class, null, null, null, out);
    return new String(out.toByteArray(), StandardCharsets.UTF_8);
  }

  @Test
  public void testWriteToEmitsJsonWithTrailingNewline() throws IOException {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("boolean", true);
    map.put("long", 42L);
    map.put("string", "value");

    String output = writeToString(map, new ByteArrayOutputStream());
    assertThat(output).isEqualTo(
        "{\"boolean\":true,\"long\":42,\"string\":\"value\"}"
            + System.getProperty("line.separator"));
  }

  @Test
  public void testWriteToDoesNotCloseUnderlyingStream() throws IOException {
    AtomicBoolean closed = new AtomicBoolean(false);
    ByteArrayOutputStream out = new ByteArrayOutputStream() {
      @Override
      public void close() {
        closed.set(true);
      }
    };

    Map<String, Object> map = new LinkedHashMap<>();
    map.put("k", "v");
    writeToString(map, out);

    assertThat(closed).isFalse();
  }
}
