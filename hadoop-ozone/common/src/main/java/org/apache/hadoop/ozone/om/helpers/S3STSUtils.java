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

package org.apache.hadoop.ozone.om.helpers;

import io.opentelemetry.api.internal.StringUtils;
import java.util.Map;

/**
 * Utility class containing constants and validation methods shared by STS endpoint and OzoneManager processing.
 */
public final class S3STSUtils {

  private S3STSUtils() {
  }

  /**
   * Adds standard AssumeRole audit params.
   */
  public static void addAssumeRoleAuditParams(Map<String, String> auditParams, String roleArn, String roleSessionName,
      String awsIamSessionPolicy, int duration, String requestId) {

    auditParams.put("action", "AssumeRole");
    auditParams.put("roleArn", roleArn);
    auditParams.put("roleSessionName", roleSessionName);
    auditParams.put("duration", String.valueOf(duration));
    auditParams.put("isPolicyIncluded", StringUtils.isNullOrEmpty(awsIamSessionPolicy) ? "N" : "Y");
    auditParams.put("requestId", requestId);
  }
}
