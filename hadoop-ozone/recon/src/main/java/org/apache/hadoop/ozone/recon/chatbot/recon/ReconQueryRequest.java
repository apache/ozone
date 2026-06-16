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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Request to fetch data from a Recon API path on behalf of the chatbot.
 */
public class ReconQueryRequest {
  private final String toolName;
  private final String method;
  private final Map<String, String> parameters;
  public ReconQueryRequest(String toolName, String method, Map<String, String> parameters) {
    this.toolName = toolName;
    this.method = method;
    this.parameters = parameters == null
        ? Collections.emptyMap()
        : Collections.unmodifiableMap(new HashMap<>(parameters));
  }

  public String getToolName() {
    return toolName;
  }

  public String getMethod() {
    return method;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }
}
