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

package org.apache.hadoop.ozone.recon.chatbot.recon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Collections;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

/** Tests for {@link ReconResponseUnwrapper}. */
public class TestReconResponseUnwrapper {

  @Test
  public void testUnwrapSuccess() throws IOException {
    Response response = Response.ok(Collections.singletonMap("key", "value")).build();
    JsonNode node = ReconResponseUnwrapper.unwrap(response);
    assertTrue(node.isObject());
    assertEquals("value", node.get("key").asText());
  }

  @Test
  public void testUnwrapNullEntity() throws IOException {
    Response response = Response.ok().build();
    JsonNode node = ReconResponseUnwrapper.unwrap(response);
    assertTrue(node.isObject());
    assertTrue(node.isEmpty());
  }

  @Test
  public void testUnwrapError() {
    Response response = Response.status(500).entity("Internal Error").build();
    IOException ex = assertThrows(IOException.class, () -> ReconResponseUnwrapper.unwrap(response));
    assertTrue(ex.getMessage().contains("500"));
    assertTrue(ex.getMessage().contains("Internal Error"));
  }
}
