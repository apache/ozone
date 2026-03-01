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

package org.apache.hadoop.ozone.om.request.validation.testvalidatorset1;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateKey;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateVolume;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.DeleteKeys;
import static org.apache.hadoop.ozone.request.validation.RequestProcessingPhase.POST_PROCESS;
import static org.apache.hadoop.ozone.request.validation.RequestProcessingPhase.PRE_PROCESS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.request.validation.OMClientVersionValidator;
import org.apache.hadoop.ozone.om.request.validation.OMLayoutVersionValidator;
import org.apache.hadoop.ozone.om.request.validation.TestRequestValidations;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Some annotated request validator method, and facilities to help check if
 * validations were properly called from tests where applicable.
 */
public final class GeneralValidatorsForTesting {

  /**
   * As the validators written here does not override any request or response
   * but throw exceptions for specific tests, a test that wants to directly
   * use a validator here, has to turn on this boolean, and the method that
   * the test relies on has to check for this value.
   *
   * This is necessary to do not affect other tests that are testing requests
   * processing, as for some of those tests this package is on the classpath
   * and therefore the annotated validations are loadede for them.
   */
  private static boolean validatorTestsRunning = false;

  private static List<ValidationListener> listeners = new ArrayList<>();

  public static void startValidatorTest() {
    validatorTestsRunning = true;
  }

  public static void finishValidatorTest() {
    validatorTestsRunning = false;
  }

  private GeneralValidatorsForTesting() { }

  /**
   * Interface to easily add listeners that get notified if a certain validator
   * method defined in this class was called.
   *
   * @see TestRequestValidations for more details on how this intercace is
   *      being used.
   */
  @FunctionalInterface
  public interface ValidationListener {
    void validationCalled(String calledMethodName);
  }

  public static void addListener(ValidationListener listener) {
    listeners.add(listener);
  }

  public static void removeListener(ValidationListener listener) {
    listeners.remove(listener);
  }

  private static void fireValidationEvent(String calledMethodName) {
    listeners.forEach(l -> l.validationCalled(calledMethodName));
  }

  @OMLayoutVersionValidator(
      applyBefore = OMLayoutFeature.QUOTA,
      processingPhase = PRE_PROCESS,
      requestType = CreateKey)
  public static OMRequest preProcessCreateKeyQuotaLayoutValidator(
      OMRequest req, ValidationContext ctx) {
    fireValidationEvent("preProcessCreateKeyQuotaLayoutValidator");
    return req;
  }

  @OMClientVersionValidator(
      applyBefore = ClientVersion.FUTURE_VERSION,
      processingPhase = PRE_PROCESS,
      requestType = CreateKey)
  public static OMRequest preProcessCreateKeyFutureClientValidator(
      OMRequest req, ValidationContext ctx) {
    fireValidationEvent("preProcessCreateKeyFutureClientValidator");
    return req;
  }

  @OMLayoutVersionValidator(
      applyBefore = OMLayoutFeature.QUOTA,
      processingPhase = POST_PROCESS,
      requestType = CreateKey)
  public static OMResponse postProcessCreateKeyQuotaLayoutValidator(
      OMRequest req, OMResponse resp, ValidationContext ctx) {
    fireValidationEvent("postProcessCreateKeyQuotaLayoutValidator");
    return resp;
  }

  @OMClientVersionValidator(
      processingPhase = PRE_PROCESS,
      requestType = CreateKey,
      applyBefore = ClientVersion.BUCKET_LAYOUT_SUPPORT)
  public static OMRequest preProcessCreateKeyBucketLayoutClientValidator(
      OMRequest req, ValidationContext ctx) {
    fireValidationEvent("preProcessCreateKeyBucketLayoutClientValidator");
    return req;
  }

  @OMClientVersionValidator(
      applyBefore = ClientVersion.BUCKET_LAYOUT_SUPPORT,
      processingPhase = POST_PROCESS,
      requestType = CreateKey)
  public static OMResponse postProcessCreateKeyBucketLayoutClientValidator(
      OMRequest req, OMResponse resp, ValidationContext ctx) {
    fireValidationEvent("postProcessCreateKeyBucketLayoutClientValidator");
    return resp;
  }

  @OMClientVersionValidator(
      applyBefore = ClientVersion.BUCKET_LAYOUT_SUPPORT,
      processingPhase = PRE_PROCESS,
      requestType = CreateVolume)
  @OMLayoutVersionValidator(
      applyBefore = OMLayoutFeature.QUOTA,
      processingPhase = PRE_PROCESS,
      requestType = CreateVolume)
  public static OMRequest multiPurposePreProcessCreateVolumeBucketLayoutCLientQuotaLayoutValidator(
      OMRequest req, ValidationContext ctx) {
    fireValidationEvent("multiPurposePreProcessCreateVolumeBucketLayoutCLientQuotaLayoutValidator");
    return req;
  }

  @OMClientVersionValidator(
      applyBefore = ClientVersion.BUCKET_LAYOUT_SUPPORT,
      processingPhase = POST_PROCESS,
      requestType = CreateVolume)
  @OMLayoutVersionValidator(
      applyBefore = OMLayoutFeature.QUOTA,
      processingPhase = POST_PROCESS,
      requestType = CreateVolume)
  public static OMResponse multiPurposePostProcessCreateVolumeBucketLayoutCLientQuotaLayoutValidator(
      OMRequest req, OMResponse resp, ValidationContext ctx) {
    fireValidationEvent("multiPurposePostProcessCreateVolumeBucketLayoutCLientQuotaLayoutValidator");
    return resp;
  }

  @OMClientVersionValidator(
      applyBefore = ClientVersion.BUCKET_LAYOUT_SUPPORT,
      processingPhase = PRE_PROCESS,
      requestType = DeleteKeys
  )
  public static OMRequest throwingPreProcessDeleteKeyBucketLayoutClientValidator(
      OMRequest req, ValidationContext ctx) throws IOException {
    fireValidationEvent("throwingPreProcessValidator");
    if (validatorTestsRunning) {
      throw new IOException("IOException: fail for testing...");
    }
    return req;
  }

  @OMClientVersionValidator(
      applyBefore = ClientVersion.BUCKET_LAYOUT_SUPPORT,
      processingPhase = POST_PROCESS,
      requestType = DeleteKeys
  )
  public static OMResponse throwingPostProcessDeleteKeyBucketLayoutClientValidator(
      OMRequest req, OMResponse resp, ValidationContext ctx)
      throws IOException {
    fireValidationEvent("throwingPostProcessValidator");
    if (validatorTestsRunning) {
      throw new IOException("IOException: fail for testing...");
    }
    return resp;
  }

}
