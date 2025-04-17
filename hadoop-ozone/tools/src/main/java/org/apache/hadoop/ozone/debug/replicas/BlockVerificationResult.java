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

package org.apache.hadoop.ozone.debug.replicas;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;

/**
 * Json structure for replicas to pass through each check and give output.
 */
public class BlockVerificationResult {

  private final String type;
  private final boolean pass;
  private final List<FailureDetail> failures;

  public BlockVerificationResult(String type, boolean pass, List<FailureDetail> failures) {
    this.type = type;
    this.pass = pass;
    this.failures = failures;
  }

  public String getType() {
    return type;
  }

  public boolean isPass() {
    return pass;
  }

  public List<FailureDetail> getFailures() {
    return failures;
  }

  /**
   * Details about the check failure.
   */
  public static class FailureDetail {
    private final boolean present;
    private final String message;

    public FailureDetail(boolean present, String message) {
      this.present = present;
      this.message = message;
    }

    public boolean isPresent() {
      return present;
    }

    public String getFailureMessage() {
      return message;
    }

  }

  public ObjectNode toJson(ObjectMapper mapper) {
    ObjectNode resultNode = mapper.createObjectNode();
    resultNode.put("type", type);
    resultNode.put("pass", pass);

    ArrayNode failuresArray = mapper.createArrayNode();
    for (FailureDetail failure : failures) {
      ObjectNode failureNode = mapper.createObjectNode();
      failureNode.put("present", failure.isPresent());
      failureNode.put("message", failure.getFailureMessage());
      failuresArray.add(failureNode);
    }

    resultNode.set("failures", failuresArray);
    return resultNode;
  }

}
