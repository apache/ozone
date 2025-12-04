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

package org.apache.hadoop.ozone.om.request.validation.testvalidatorset2;

import static org.apache.hadoop.ozone.om.request.validation.ValidationCondition.OLDER_CLIENT_REQUESTS;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateKey;
import static org.apache.hadoop.ozone.request.validation.RequestProcessingPhase.PRE_PROCESS;

import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;

/**
 * Separate validator methods for a few specific tests that covers cases where
 * there are almost no validators added.
 */
public final class ValidatorsForOnlyOldClientValidations {

  private ValidatorsForOnlyOldClientValidations() { }

  @RequestFeatureValidator(
      conditions = { OLDER_CLIENT_REQUESTS },
      processingPhase = PRE_PROCESS,
      requestType = CreateKey)
  public static OMRequest oldClientPreProcessCreateKeyValidator2(
      OMRequest req, ValidationContext ctx) {
    return req;
  }
}
