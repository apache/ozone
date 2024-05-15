/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds;

import com.fasterxml.jackson.annotation.JsonInclude;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;

/**
 * JSON Utility functions used in ozone for Test classes.
 */
public final class JsonTestUtils {

  // Reuse ObjectMapper instance for improving performance.
  // ObjectMapper is thread safe as long as we always configure instance
  // before use.
  private static final ObjectMapper MAPPER;
  private static final ObjectWriter WRITER;

  static {
    MAPPER = new ObjectMapper()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .registerModule(new JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    WRITER = MAPPER.writerWithDefaultPrettyPrinter();
  }

  private JsonTestUtils() {
    // Never constructed
  }

  public static String toJsonStringWithDefaultPrettyPrinter(Object obj)
      throws IOException {
    return WRITER.writeValueAsString(obj);
  }

  public static String toJsonString(Object obj) throws IOException {
    return MAPPER.writeValueAsString(obj);
  }

  public static JsonNode valueToJsonNode(Object value) {
    return MAPPER.valueToTree(value);
  }

  public static JsonNode readTree(String content) throws IOException {
    return MAPPER.readTree(content);
  }
}
