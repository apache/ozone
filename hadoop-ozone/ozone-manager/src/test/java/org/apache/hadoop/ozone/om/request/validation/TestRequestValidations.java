/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.request.validation;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.request.validation.testvalidatorset1.GeneralValidatorsForTesting;
import org.apache.hadoop.ozone.om.request.validation.testvalidatorset1.GeneralValidatorsForTesting.ValidationListener;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.ozone.om.request.validation.ValidationContext.of;
import static org.apache.hadoop.ozone.om.request.validation.testvalidatorset1.GeneralValidatorsForTesting.startValidatorTest;
import static org.apache.hadoop.ozone.om.request.validation.testvalidatorset1.GeneralValidatorsForTesting.finishValidatorTest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateKey;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.DeleteKeys;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.RenameKey;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

  @Before
  public void setup() {
    startValidatorTest();
    validationListener.attach();
  }

  @After
  public void tearDown() {
    validationListener.detach();
    finishValidatorTest();
  }

  @Test(expected = NullPointerException.class)
  public void testUsingRegistryWithoutLoading() throws ServiceException {
    new RequestValidations()
        .fromPackage(PACKAGE)
        .withinContext(of(aFinalizedVersionManager()))
        .validateRequest(aCreateKeyRequest(currentClientVersion()));
  }

  @Test(expected = NullPointerException.class)
  public void testUsingRegistryWithoutContext() throws ServiceException {
    new RequestValidations()
        .fromPackage(PACKAGE)
        .load()
        .validateRequest(aCreateKeyRequest(currentClientVersion()));
  }

  @Test
  public void testUsingRegistryWithoutPackage() throws ServiceException {
    new RequestValidations()
        .withinContext(of(aFinalizedVersionManager()))
        .load()
        .validateRequest(aCreateKeyRequest(currentClientVersion()));

    validationListener.assertNumOfEvents(0);
  }

  @Test
  public void testNoPreValidationsWithoutValidationMethods()
      throws ServiceException {
    int omVersion = 0;
    ValidationContext ctx = of(aFinalizedVersionManager());
    RequestValidations validations = loadEmptyValidations(ctx);

    validations.validateRequest(aCreateKeyRequest(omVersion));

    validationListener.assertNumOfEvents(0);
  }

  @Test
  public void testNoPostValidationsWithoutValidationMethods()
      throws ServiceException {
    ValidationContext ctx = of(aFinalizedVersionManager());
    RequestValidations validations = loadEmptyValidations(ctx);

    validations.validateResponse(
        aCreateKeyRequest(currentClientVersion()), aCreateKeyResponse());

    validationListener.assertNumOfEvents(0);
  }

  @Test
  public void testNoPreValidationsRunningForRequestTypeWithoutValidators()
      throws ServiceException {
    ValidationContext ctx = of(aFinalizedVersionManager());
    RequestValidations validations = loadValidations(ctx);

    validations.validateRequest(aRenameKeyRequest(currentClientVersion()));

    validationListener.assertNumOfEvents(0);
  }

  @Test
  public void testNoPostValidationsAreRunningForRequestTypeWithoutValidators()
      throws ServiceException {
    ValidationContext ctx = of(aFinalizedVersionManager());
    RequestValidations validations = loadValidations(ctx);

    validations.validateResponse(
        aRenameKeyRequest(currentClientVersion()), aRenameKeyResponse());

    validationListener.assertNumOfEvents(0);
  }

  @Test
  public void testPreProcessorExceptionHandling() {
    ValidationContext ctx = of(aFinalizedVersionManager());
    RequestValidations validations = loadValidations(ctx);

    try {
      validations.validateRequest(aDeleteKeysRequest(olderClientVersion()));
      fail("ServiceException was expected but was not thrown.");
    } catch (ServiceException ignored) { }

    validationListener.assertNumOfEvents(1);
    validationListener.assertExactListOfValidatorsCalled(
        "throwingPreProcessValidator");
  }

  @Test
  public void testPostProcessorExceptionHandling() {
    ValidationContext ctx = of(aFinalizedVersionManager());
    RequestValidations validations = loadValidations(ctx);

    try {
      validations.validateResponse(
          aDeleteKeysRequest(olderClientVersion()), aDeleteKeysResponse());
      fail("ServiceException was expected but was not thrown.");
    } catch (ServiceException ignored) { }

    validationListener.assertNumOfEvents(1);
    validationListener.assertExactListOfValidatorsCalled(
        "throwingPostProcessValidator");
  }

  @Test
  public void testOldClientConditionIsRecognizedAndPreValidatorsApplied()
      throws ServiceException {
    ValidationContext ctx = of(aFinalizedVersionManager());
    RequestValidations validations = loadValidations(ctx);

    validations.validateRequest(aCreateKeyRequest(olderClientVersion()));

    validationListener.assertNumOfEvents(1);
    validationListener.assertExactListOfValidatorsCalled(
        "oldClientPreProcessCreateKeyValidator");
  }

  @Test
  public void testOldClientConditionIsRecognizedAndPostValidatorsApplied()
      throws ServiceException {
    ValidationContext ctx = of(aFinalizedVersionManager());
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
      throws ServiceException {
    ValidationContext ctx = of(anUnfinalizedVersionManager());
    RequestValidations validations = loadValidations(ctx);

    validations.validateRequest(aCreateKeyRequest(olderClientVersion()));

    validationListener.assertNumOfEvents(2);
    validationListener.assertExactListOfValidatorsCalled(
        "preFinalizePreProcessCreateKeyValidator",
        "oldClientPreProcessCreateKeyValidator");
  }

  @Test
  public void testPreFinalizedWithOldClientConditionPostProcValidatorsApplied()
      throws ServiceException {
    ValidationContext ctx = of(anUnfinalizedVersionManager());
    RequestValidations validations = loadValidations(ctx);

    validations.validateResponse(
        aCreateKeyRequest(olderClientVersion()), aCreateKeyResponse());

    validationListener.assertNumOfEvents(3);
    validationListener.assertExactListOfValidatorsCalled(
        "preFinalizePostProcessCreateKeyValidator",
        "oldClientPostProcessCreateKeyValidator",
        "oldClientPostProcessCreateKeyValidator2");
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
    return ClientVersion.CURRENT_VERSION - 1;
  }

  private int currentClientVersion() {
    return ClientVersion.CURRENT_VERSION;
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


