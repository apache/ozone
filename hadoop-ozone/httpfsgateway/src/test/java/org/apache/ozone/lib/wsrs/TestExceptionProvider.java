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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.FileNotFoundException;
import java.util.Map;
import org.apache.hadoop.util.HttpExceptionUtils;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ExceptionProvider}.
 */
public class TestExceptionProvider {

  @Test
  public void getOneLineMessageTruncatesAtFirstNewline() {
    ExceptionProvider ep = new ExceptionProvider();
    assertEquals("first line",
        ep.getOneLineMessage(new RuntimeException("first line\nsecond line\nthird line")));
  }

  @Test
  public void getOneLineMessagePreservesSingleLine() {
    ExceptionProvider ep = new ExceptionProvider();
    assertEquals("single line",
        ep.getOneLineMessage(new RuntimeException("single line")));
  }

  @Test
  public void getOneLineMessageNullMessageReturnsNull() {
    ExceptionProvider ep = new ExceptionProvider();
    assertNull(ep.getOneLineMessage(new RuntimeException((String) null)));
  }

  /**
   * Verifies that {@link ExceptionProvider#createResponse} produces the
   * WebHDFS RemoteException JSON contract that clients (e.g.
   * {@code WebHdfsFileSystem}) rely on to reconstruct typed exceptions.
   * Also confirms that a multi-line exception message is truncated to its
   * first line before embedding in the response.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void createResponseBuildsWebHdfsRemoteExceptionContract() {
    ExceptionProvider ep = new ExceptionProvider();
    Response r = ep.createResponse(Response.Status.NOT_FOUND,
        new FileNotFoundException("first line\nsecond line"));

    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), r.getStatus());
    assertTrue(r.getMediaType().isCompatible(MediaType.APPLICATION_JSON_TYPE),
        "response media type must be application/json");

    // Outer map: { "RemoteException": { ... } }
    Map<String, Object> outer = (Map<String, Object>) r.getEntity();
    assertTrue(outer.containsKey(HttpExceptionUtils.ERROR_JSON),
        "entity must contain " + HttpExceptionUtils.ERROR_JSON + " key");

    // Inner map carries the three fields WebHdfsFileSystem reads.
    Map<String, Object> inner =
        (Map<String, Object>) outer.get(HttpExceptionUtils.ERROR_JSON);
    assertEquals("FileNotFoundException",
        inner.get(HttpExceptionUtils.ERROR_EXCEPTION_JSON));
    assertEquals("java.io.FileNotFoundException",
        inner.get(HttpExceptionUtils.ERROR_CLASSNAME_JSON));
    // Multi-line message must be truncated to the first line.
    assertEquals("first line",
        inner.get(HttpExceptionUtils.ERROR_MESSAGE_JSON));
  }
}
