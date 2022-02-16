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
import org.apache.hadoop.ozone.om.request.validation.GeneralValidatorsForTesting.ValidationListener;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import sun.jvm.hotspot.utilities.AssertionFailure;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.ozone.om.request.validation.ValidationContext.of;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.*;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateKey;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.DeleteKeys;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.RenameKey;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRequestValidations {
  private static final String PACKAGE =
      "org.apache.hadoop.ozone.om.request.validation";

  private static final String PACKAGE_WO_VALIDATORS =
      "org.apache.hadoop.hdds.annotation";

  private final ValidationListenerImpl validationListener =
      new ValidationListenerImpl();

  @Before
  public void setup() {
    validationListener.attach();
  }

  @After
  public void tearDown() {
    validationListener.detach();
  }

  @Test(expected = NullPointerException.class)
  public void testUsingRegistryWithoutLoading() throws ServiceException {
    new RequestValidations()
        .fromPackage(PACKAGE)
        .withinContext(of(aFinalizedVersionManager(), 0))
        .validateRequest(aCreateKeyRequest(0));
  }

  @Test(expected = NullPointerException.class)
  public void testUsingRegistryWithoutContext() throws ServiceException {
    new RequestValidations()
        .fromPackage(PACKAGE)
        .load()
        .validateRequest(aCreateKeyRequest(0));
  }

  @Test
  public void testUsingRegistryWithoutPackage() throws ServiceException {
    new RequestValidations()
        .withinContext(of(aFinalizedVersionManager(), 0))
        .load()
        .validateRequest(aCreateKeyRequest(0));

    validationListener.assertNumOfEvents(1);
    validationListener
        .assertCalled("unconditionalPreProcessCreateKeyValidator");
  }

  @Test
  public void testNoPreValdiationsWithoutValidationMethods()
      throws ServiceException {
    int omVersion = 0;
    ValidationContext ctx = of(aFinalizedVersionManager(), omVersion);
    RequestValidations validations = loadEmptyValidations(ctx);

    validations.validateRequest(aCreateKeyRequest(omVersion));

    validationListener.assertNumOfEvents(0);
  }

  @Test
  public void testNoPostValdiationsWithoutValidationMethods()
      throws ServiceException {
    int omVersion = 0;
    ValidationContext ctx = of(aFinalizedVersionManager(), omVersion);
    RequestValidations validations = loadEmptyValidations(ctx);

    validations
        .validateResponse(aCreateKeyRequest(omVersion), aCreateKeyResponse());

    validationListener.assertNumOfEvents(0);
  }

  @Test
  public void testNoPreValidationsRunningForRequestTypeWithoutValidators()
      throws ServiceException {
    int omVersion = 0;
    ValidationContext ctx = of(aFinalizedVersionManager(), omVersion);
    RequestValidations validations = loadValidations(ctx);

    validations.validateRequest(aRenameKeyRequest(omVersion));

    validationListener.assertNumOfEvents(0);
  }

  @Test
  public void testNoPostValidationsAreRunningForRequestTypeWithoutValidators()
      throws ServiceException {
    int omVersion = 0;
    ValidationContext ctx = of(aFinalizedVersionManager(), omVersion);
    RequestValidations validations = loadValidations(ctx);

    validations.
        validateResponse(aRenameKeyRequest(omVersion), aRenameKeyResponse());

    validationListener.assertNumOfEvents(0);
  }

  @Test
  public void testUnconditionalPreProcessValidationsAreCalled()
      throws ServiceException {
    int omVersion = 0;
    ValidationContext ctx = of(aFinalizedVersionManager(), omVersion);
    RequestValidations validations = loadValidations(ctx);

    validations.validateRequest(aCreateKeyRequest(omVersion));

    validationListener.assertNumOfEvents(1);
    validationListener
        .assertCalled("unconditionalPreProcessCreateKeyValidator");
  }

  @Test
  public void testUnconditionalPostProcessValidationsAreCalled()
      throws ServiceException {
    int omVersion = 0;
    ValidationContext ctx = of(aFinalizedVersionManager(), omVersion);
    RequestValidations validations = loadValidations(ctx);

    validations.
        validateResponse(aCreateKeyRequest(omVersion), aCreateKeyResponse());

    validationListener.assertNumOfEvents(1);
    validationListener
        .assertCalled("unconditionalPostProcessCreateKeyValidator");
  }

  @Test
  public void testPreProcessorExceptionHandling() {
    int omVersion = 0;
    ValidationContext ctx = of(aFinalizedVersionManager(), omVersion);
    RequestValidations validations = loadValidations(ctx);

    try {
      validations.validateRequest(aDeleteKeysRequest(omVersion));
      Assert.fail("ServiceException was expected but was not thrown.");
    } catch (ServiceException ignored) {}

    validationListener.assertNumOfEvents(1);
    validationListener.assertCalled("throwingPreProcessValidator");
  }

  @Test
  public void testPostProcessorExceptionHandling() {
    int omVersion = 0;
    ValidationContext ctx = of(aFinalizedVersionManager(), omVersion);
    RequestValidations validations = loadValidations(ctx);

    try {
      validations.validateResponse(
          aDeleteKeysRequest(omVersion), aDeleteKeysResponse());
      Assert.fail("ServiceException was expected but was not thrown.");
    } catch (ServiceException ignored) {}

    validationListener.assertNumOfEvents(1);
    validationListener.assertCalled("throwingPostProcessValidator");
  }

  @Test
  public void testNewClientConditionIsRecognizedAndPreValidatorsApplied()
      throws ServiceException {
    int omVersion = 0;
    ValidationContext ctx = of(aFinalizedVersionManager(), omVersion);
    RequestValidations validations = loadValidations(ctx);

    validations.validateRequest(aCreateKeyRequest(omVersion + 1));

    validationListener.assertNumOfEvents(2);
    validationListener.assertAllCalled(
        "unconditionalPreProcessCreateKeyValidator",
        "newClientPreProcessCreateKeyValidator");
  }

  @Test
  public void testNewClientConditionIsRecognizedAndPostValidatorsApplied()
      throws ServiceException {
    int omVersion = 0;
    ValidationContext ctx = of(aFinalizedVersionManager(), omVersion);
    RequestValidations validations = loadValidations(ctx);

    validations.validateResponse(
        aCreateKeyRequest(omVersion + 1), aCreateKeyResponse());

    validationListener.assertNumOfEvents(3);
    validationListener.assertAllCalled(
        "unconditionalPostProcessCreateKeyValidator",
        "newClientPostProcessCreateKeyValidator",
        "newClientPostProcessCreateKeyValidator2");
  }

  @Test
  public void testOldClientConditionIsRecognizedAndPreValidatorsApplied()
      throws ServiceException {
    int omVersion = 2;
    ValidationContext ctx = of(aFinalizedVersionManager(), omVersion);
    RequestValidations validations = loadValidations(ctx);

    validations.validateRequest(aCreateKeyRequest(omVersion - 1));

    validationListener.assertNumOfEvents(2);
    validationListener.assertAllCalled(
        "unconditionalPreProcessCreateKeyValidator",
        "oldClientPreProcessCreateKeyValidator");
  }

  @Test
  public void testOldClientConditionIsRecognizedAndPostValidatorsApplied()
      throws ServiceException {
    int omVersion = 2;
    ValidationContext ctx = of(aFinalizedVersionManager(), omVersion);
    RequestValidations validations = loadValidations(ctx);

    validations.validateResponse(
        aCreateKeyRequest(omVersion - 1), aCreateKeyResponse());

    validationListener.assertNumOfEvents(2);
    validationListener.assertAllCalled(
        "unconditionalPostProcessCreateKeyValidator",
        "oldClientPostProcessCreateKeyValidator");
  }

  @Test
  public void
  testPreFinalizedWithOldClientConditionIsRecognizedAndPreValidatorsApplied()
      throws ServiceException {
    int omVersion = 2;
    ValidationContext ctx = of(anUnfinalizedVersionManager(), omVersion);
    RequestValidations validations = loadValidations(ctx);

    validations.validateRequest(aCreateKeyRequest(omVersion - 1));

    validationListener.assertNumOfEvents(3);
    validationListener.assertAllCalled(
        "unconditionalPreProcessCreateKeyValidator",
        "preFinalizePreProcessCreateKeyValidator",
        "oldClientPreProcessCreateKeyValidator");
  }

  @Test
  public void
  testPreFinalizedWithOldClientConditionIsRecognizedAndPostValidatorsApplied()
      throws ServiceException {
    int omVersion = 2;
    ValidationContext ctx = of(anUnfinalizedVersionManager(), omVersion);
    RequestValidations validations = loadValidations(ctx);

    validations.validateResponse(
        aCreateKeyRequest(omVersion - 1), aCreateKeyResponse());

    validationListener.assertNumOfEvents(3);
    validationListener.assertAllCalled(
        "unconditionalPostProcessCreateKeyValidator",
        "preFinalizePostProcessCreateKeyValidator",
        "oldClientPostProcessCreateKeyValidator");
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

  private OMRequest aCreateKeyRequest(int version) {
    return aRequest(CreateKey, version);
  }

  private OMRequest aDeleteKeysRequest(int version) {
    return aRequest(DeleteKeys, version);
  }

  private OMRequest aRenameKeyRequest(int version) {
    return aRequest(RenameKey, version);
  }

  private OMRequest aRequest(Type type, int version) {
    return OMRequest.newBuilder()
        .setVersion(version)
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
    List<String> calledMethods = new ArrayList<>();

    @Override
    public void validationCalled(String calledMethodName) {
      calledMethods.add(calledMethodName);
    }

    public void attach(){
      GeneralValidatorsForTesting.addListener(this);
    }

    public void detach() {
      GeneralValidatorsForTesting.removeListener(this);
      reset();
    }

    public void reset() {
      calledMethods = new ArrayList<>();
    }

    public void assertCalled(String... methodNames) {
      for (String methodName : methodNames) {
        if (!calledMethods.contains(methodName)) {
          throw new AssertionFailure("Expected method call for " + methodName
              + " did not happened.");
        }
      }
    }

    public void assertAllCalled(String... methodNames) {
      List<String> calls = new ArrayList<>(calledMethods);
      for (String methodName : methodNames) {
        if (!calls.remove(methodName)) {
          throw new AssertionFailure("Expected method call for " + methodName
              + " did not happened.");
        }
      }
      if (!calls.isEmpty()) {
        throw new AssertionFailure("Some of the methods were not called."
            + "Missing calls for: " + calls);
      }
    }

    public void assertNumOfEvents(int count) {
      if (calledMethods.size() != count) {
        throw new AssertionFailure("Unexpected validation call count."
            + " Expected: " + count + "; Happened: " + calledMethods.size());
      }
    }
  }
}


