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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import javax.ws.rs.core.Response;

/**
 * Converts a JAX-RS Response entity to JsonNode for the chatbot pipeline.
 */
public final class ReconResponseUnwrapper {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ReconResponseUnwrapper() {
    // Utility class
  }

  public static JsonNode unwrap(Response response) throws IOException {
    if (response == null) {
      return MAPPER.createObjectNode();
    }

    int status = response.getStatus();
    if (status < 200 || status >= 300) {
      String errorMsg = "API request failed with status " + status;
      if (response.getEntity() != null) {
        errorMsg += ": " + response.getEntity().toString();
      }
      throw new IOException(errorMsg);
    }

    Object entity = response.getEntity();
    if (entity == null) {
      return MAPPER.createObjectNode();
    }

    return MAPPER.valueToTree(entity);
  }
}
