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

package org.apache.hadoop.ozone.om.request.validation;

import static org.apache.hadoop.ozone.om.request.validation.ValidationContext.of;
import static org.apache.hadoop.ozone.om.request.validation.testvalidatorset1.GeneralValidatorsForTesting.finishValidatorTest;
import static org.apache.hadoop.ozone.om.request.validation.testvalidatorset1.GeneralValidatorsForTesting.startValidatorTest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateKey;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.DeleteKeys;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.RenameKey;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.validation.testvalidatorset1.GeneralValidatorsForTesting;
import org.apache.hadoop.ozone.om.request.validation.testvalidatorset1.GeneralValidatorsForTesting.ValidationListener;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Testing the RequestValidations class that is used to run the validation for
 * any given request that arrives to OzoneManager.
 */
public class TestRequestValidations {
  private static final String PACKAGE =
      "org.apache.hadoop.ozone.om.request.validation.testvalidatorset1";

  private static final String PACKAGE_WO_VALIDATORS =
      "org.apache.hadoop.hdds.annotation";

  private final ValidationListenerImpl validationListener =
      new ValidationListenerImpl();

  private OMMetadataManager metadataManager;

  @BeforeEach
  public void setup() {
    metadataManager = mock(OMMetadataManager.class);
    startValidatorTest();
    validationListener.attach();
  }

  @AfterEach
  public void tearDown() {
    validationListener.detach();
    finishValidatorTest();
  }

  @Test
  public void testUsingRegistryWithoutLoading() throws Exception {
    assertThrows(NullPointerException.class, () -> {
      new RequestValidations()
          .fromPackage(PACKAGE)
          .withinContext(of(aFinalizedVersionManager(), metadataManager))
          .validateRequest(aCreateKeyRequest(currentClientVersion()));
    });
  }

  @Test
  public void testUsingRegistryWithoutContext() throws Exception {
    assertThrows(NullPointerException.class, () -> {
      new RequestValidations()
          .fromPackage(PACKAGE)
          .load()
          .validateRequest(aCreateKeyRequest(currentClientVersion()));
    });
  }

  @Test
  public void testUsingRegistryWithoutPackage() throws Exception {
    new RequestValidations()
        .withinContext(of(aFinalizedVersionManager(), metadataManager))
        .load()
        .validateRequest(aCreateKeyRequest(currentClientVersion()));

    validationListener.assertNumOfEvents(0);
  }

  @Test
  public void testNoPreValidationsWithoutValidationMethods()
      throws Exception {
    int omVersion = 0;
    ValidationContext ctx = of(aFinalizedVersionManager(), metadataManager);
    RequestValidations validations = loadEmptyValidations(ctx);

    validations.validateRequest(aCreateKeyRequest(omVersion));

    validationListener.assertNumOfEvents(0);
  }

  @Test
  public void testNoPostValidationsWithoutValidationMethods()
      throws Exception {
    ValidationContext ctx = of(aFinalizedVersionManager(), metadataManager);
    RequestValidations validations = loadEmptyValidations(ctx);

    validations.validateResponse(
        aCreateKeyRequest(currentClientVersion()), aCreateKeyResponse());

    validationListener.assertNumOfEvents(0);
  }

  @Test
  public void testNoPreValidationsRunningForRequestTypeWithoutValidators()
      throws Exception {
    ValidationContext ctx = of(aFinalizedVersionManager(), metadataManager);
    RequestValidations validations = loadValidations(ctx);

    validations.validateRequest(aRenameKeyRequest(currentClientVersion()));

    validationListener.assertNumOfEvents(0);
  }

  @Test
  public void testNoPostValidationsAreRunningForRequestTypeWithoutValidators()
      throws Exception {
    ValidationContext ctx = of(aFinalizedVersionManager(), metadataManager);
    RequestValidations validations = loadValidations(ctx);

    validations.validateResponse(
        aRenameKeyRequest(currentClientVersion()), aRenameKeyResponse());

    validationListener.assertNumOfEvents(0);
  }

  @Test
  public void testPreProcessorExceptionHandling() throws Exception {
    ValidationContext ctx = of(aFinalizedVersionManager(), metadataManager);
    RequestValidations validations = loadValidations(ctx);
    assertThrows(Exception.class,
        () -> validations.validateRequest(aDeleteKeysRequest(olderClientVersion())));

    validationListener.assertNumOfEvents(1);
    validationListener.assertExactListOfValidatorsCalled(
        "throwingPreProcessValidator");
  }

  @Test
  public void testPostProcessorExceptionHandling() {
    ValidationContext ctx = of(aFinalizedVersionManager(), metadataManager);
    RequestValidations validations = loadValidations(ctx);
    assertThrows(Exception.class,
        () -> validations.validateResponse(aDeleteKeysRequest(olderClientVersion()), aDeleteKeysResponse()));

    validationListener.assertNumOfEvents(1);
    validationListener.assertExactListOfValidatorsCalled(
        "throwingPostProcessValidator");
  }

  @Test
  public void testOldClientConditionIsRecognizedAndPreValidatorsApplied()
      throws Exception {
    ValidationContext ctx = of(aFinalizedVersionManager(), metadataManager);
    RequestValidations validations = loadValidations(ctx);

    validations.validateRequest(aCreateKeyRequest(olderClientVersion()));

    validationListener.assertNumOfEvents(1);
    validationListener.assertExactListOfValidatorsCalled(
        "oldClientPreProcessCreateKeyValidator");
  }

  @Test
  public void testOldClientConditionIsRecognizedAndPostValidatorsApplied()
      throws Exception {
    ValidationContext ctx = of(aFinalizedVersionManager(), metadataManager);
    RequestValidations validations = loadValidations(ctx);

    validations.validateResponse(
        aCreateKeyRequest(olderClientVersion()), aCreateKeyResponse());

    validationListener.assertNumOfEvents(2);
    validationListener.assertExactListOfValidatorsCalled(
        "oldClientPostProcessCreateKeyValidator",
        "oldClientPostProcessCreateKeyValidator2");
  }

  @Test
  public void testPreFinalizedWithOldClientConditionPreProcValidatorsApplied()
      throws Exception {
    ValidationContext ctx = of(anUnfinalizedVersionManager(), metadataManager);
    RequestValidations validations = loadValidations(ctx);

    validations.validateRequest(aCreateKeyRequest(olderClientVersion()));

    validationListener.assertNumOfEvents(2);
    validationListener.assertExactListOfValidatorsCalled(
        "preFinalizePreProcessCreateKeyValidator",
        "oldClientPreProcessCreateKeyValidator");
  }

  @Test
  public void testPreFinalizedWithOldClientConditionPostProcValidatorsApplied()
      throws Exception {
    ValidationContext ctx = of(anUnfinalizedVersionManager(), metadataManager);
    RequestValidations validations = loadValidations(ctx);

    validations.validateResponse(
        aCreateKeyRequest(olderClientVersion()), aCreateKeyResponse());

    validationListener.assertNumOfEvents(3);
    validationListener.assertExactListOfValidatorsCalled(
        "preFinalizePostProcessCreateKeyValidator",
        "oldClientPostProcessCreateKeyValidator",
        "oldClientPostProcessCreateKeyValidator2");
  }

  /**
   * Validates the getBucketLayout hook present in the Context object for use
   * by the validators.
   *
   * @throws Exception
   */
  @Test
  public void testValidationContextGetBucketLayout()
      throws Exception {
    ValidationContext ctx = of(anUnfinalizedVersionManager(), metadataManager);

    String volName = "vol-1";
    String buckName = "buck-1";

    String buckKey = volName + OzoneConsts.OZONE_URI_DELIMITER + buckName;
    when(metadataManager.getBucketKey(volName, buckName)).thenReturn(buckKey);

    Table<String, OmBucketInfo> buckTable = mock(Table.class);
    when(metadataManager.getBucketTable()).thenReturn(buckTable);

    OmBucketInfo buckInfo = mock(OmBucketInfo.class);
    when(buckTable.get(buckKey)).thenReturn(buckInfo);

    // No need to simulate link bucket for this test.
    when(buckInfo.isLink()).thenReturn(false);

    when(buckInfo.getBucketLayout())
        .thenReturn(BucketLayout.FILE_SYSTEM_OPTIMIZED);

    BucketLayout buckLayout = ctx.getBucketLayout("vol-1", "buck-1");
    assertTrue(buckLayout.isFileSystemOptimized());
  }

  private RequestValidations loadValidations(ValidationContext ctx) {
    return new RequestValidations()
        .fromPackage(PACKAGE)
        .withinContext(ctx)
        .load();
  }

  private RequestValidations loadEmptyValidations(ValidationContext ctx) {
    return new RequestValidations()
        .fromPackage(PACKAGE_WO_VALIDATORS)
        .withinContext(ctx)
        .load();
  }

  private int olderClientVersion() {
    return ClientVersion.CURRENT.serialize() - 1;
  }

  private int currentClientVersion() {
    return ClientVersion.CURRENT.serialize();
  }

  private OMRequest aCreateKeyRequest(int clientVersion) {
    return aRequest(CreateKey, clientVersion);
  }

  private OMRequest aDeleteKeysRequest(int clientVersion) {
    return aRequest(DeleteKeys, clientVersion);
  }

  private OMRequest aRenameKeyRequest(int clientVersion) {
    return aRequest(RenameKey, clientVersion);
  }

  private OMRequest aRequest(Type type, int clientVersion) {
    return OMRequest.newBuilder()
        .setVersion(clientVersion)
        .setCmdType(type)
        .setClientId("TestClient")
        .build();
  }

  private OMResponse aCreateKeyResponse() {
    return aResponse(CreateKey);
  }

  private OMResponse aDeleteKeysResponse() {
    return aResponse(DeleteKeys);
  }

  private OMResponse aRenameKeyResponse() {
    return aResponse(RenameKey);
  }

  private OMResponse aResponse(Type type) {
    return OMResponse.newBuilder()
        .setCmdType(type)
        .setStatus(OK)
        .build();
  }

  private LayoutVersionManager aFinalizedVersionManager() {
    LayoutVersionManager vm = mock(LayoutVersionManager.class);
    when(vm.needsFinalization()).thenReturn(false);
    return vm;
  }

  private LayoutVersionManager anUnfinalizedVersionManager() {
    LayoutVersionManager vm = mock(LayoutVersionManager.class);
    when(vm.needsFinalization()).thenReturn(true);
    return vm;
  }

  private static class ValidationListenerImpl implements ValidationListener {
    private List<String> calledMethods = new ArrayList<>();

    @Override
    public void validationCalled(String calledMethodName) {
      calledMethods.add(calledMethodName);
    }

    public void attach() {
      GeneralValidatorsForTesting.addListener(this);
    }

    public void detach() {
      GeneralValidatorsForTesting.removeListener(this);
      reset();
    }

    public void reset() {
      calledMethods = new ArrayList<>();
    }

    public void assertExactListOfValidatorsCalled(String... methodNames) {
      List<String> calls = new ArrayList<>(calledMethods);
      for (String methodName : methodNames) {
        if (!calls.remove(methodName)) {
          fail("Expected method call for " + methodName + " did not happened.");
        }
      }
      if (!calls.isEmpty()) {
        fail("Some of the methods were not called."
            + "Missing calls for: " + calls);
      }
    }

    public void assertNumOfEvents(int count) {
      if (calledMethods.size() != count) {
        fail("Unexpected validation call count."
            + " Expected: " + count + "; Happened: " + calledMethods.size());
      }
    }
  }
}


