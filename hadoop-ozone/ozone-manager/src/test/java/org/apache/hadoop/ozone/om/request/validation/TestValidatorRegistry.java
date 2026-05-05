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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.hadoop.ozone.om.request.validation.ValidationCondition.CLUSTER_NEEDS_FINALIZATION;
import static org.apache.hadoop.ozone.om.request.validation.ValidationCondition.OLDER_CLIENT_REQUESTS;
import static org.apache.hadoop.ozone.om.request.validation.testvalidatorset1.GeneralValidatorsForTesting.finishValidatorTest;
import static org.apache.hadoop.ozone.om.request.validation.testvalidatorset1.GeneralValidatorsForTesting.startValidatorTest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateDirectory;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateKey;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateVolume;
import static org.apache.hadoop.ozone.request.validation.RequestProcessingPhase.POST_PROCESS;
import static org.apache.hadoop.ozone.request.validation.RequestProcessingPhase.PRE_PROCESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reflections.util.ClasspathHelper;

/**
 * Validator registry tests.
 * For validator method declarations see the GeneralValidatorsForTesting
 * and ValidatorsForOnlyNewClientValidations (in ../avalidation2) classes.
 */
public class TestValidatorRegistry {
  private static final String PACKAGE =
      "org.apache.hadoop.ozone.om.request.validation.testvalidatorset1";

  private static final String PACKAGE2 =
      "org.apache.hadoop.ozone.om.request.validation.testvalidatorset2";

  private static final String PACKAGE_WO_VALIDATORS =
      "org.apache.hadoop.hdds.annotation";

  @BeforeEach
  public void setup() {
    startValidatorTest();
  }

  @AfterEach
  public void tearDown() {
    finishValidatorTest();
  }

  @Test
  public void testNoValidatorsReturnedForEmptyConditionList() {
    ValidatorRegistry registry = new ValidatorRegistry(PACKAGE);
    List<Method> validators =
        registry.validationsFor(emptyList(), CreateKey, PRE_PROCESS);

    assertTrue(validators.isEmpty());
  }

  @Test
  public void testRegistryHasThePreFinalizePreProcessCreateKeyValidator() {
    ValidatorRegistry registry = new ValidatorRegistry(PACKAGE);
    List<Method> validators =
        registry.validationsFor(
            asList(CLUSTER_NEEDS_FINALIZATION), CreateKey, PRE_PROCESS);

    assertEquals(1, validators.size());
    String expectedMethodName = "preFinalizePreProcessCreateKeyValidator";
    assertEquals(expectedMethodName, validators.get(0).getName());
  }

  @Test
  public void testRegistryHasThePreFinalizePostProcessCreateKeyValidator() {
    ValidatorRegistry registry = new ValidatorRegistry(PACKAGE);
    List<Method> validators =
        registry.validationsFor(
            asList(CLUSTER_NEEDS_FINALIZATION), CreateKey, POST_PROCESS);

    assertEquals(1, validators.size());
    String expectedMethodName = "preFinalizePostProcessCreateKeyValidator";
    assertEquals(expectedMethodName, validators.get(0).getName());
  }

  @Test
  public void testRegistryHasTheOldClientPreProcessCreateKeyValidator() {
    ValidatorRegistry registry = new ValidatorRegistry(PACKAGE);
    List<Method> validators =
        registry.validationsFor(
            asList(OLDER_CLIENT_REQUESTS), CreateKey, PRE_PROCESS);

    assertEquals(2, validators.size());
    List<String> methodNames =
        validators.stream().map(Method::getName).collect(Collectors.toList());
    assertThat(methodNames).contains("oldClientPreProcessCreateKeyValidator");
    assertThat(methodNames).contains("oldClientPreProcessCreateKeyValidator2");
  }

  @Test
  public void testRegistryHasTheOldClientPostProcessCreateKeyValidator() {
    ValidatorRegistry registry = new ValidatorRegistry(PACKAGE);
    List<Method> validators =
        registry.validationsFor(
            asList(OLDER_CLIENT_REQUESTS), CreateKey, POST_PROCESS);

    assertEquals(2, validators.size());
    List<String> methodNames =
        validators.stream().map(Method::getName).collect(Collectors.toList());
    assertThat(methodNames).contains("oldClientPostProcessCreateKeyValidator");
    assertThat(methodNames).contains("oldClientPostProcessCreateKeyValidator2");
  }

  @Test
  public void testRegistryHasTheMultiPurposePreProcessCreateVolumeValidator() {
    ValidatorRegistry registry = new ValidatorRegistry(PACKAGE);
    List<Method> preFinalizeValidators =
        registry.validationsFor(
            asList(CLUSTER_NEEDS_FINALIZATION), CreateVolume, PRE_PROCESS);
    List<Method> newClientValidators =
        registry.validationsFor(
            asList(OLDER_CLIENT_REQUESTS), CreateVolume, PRE_PROCESS);

    assertEquals(1, preFinalizeValidators.size());
    assertEquals(1, newClientValidators.size());
    String expectedMethodName = "multiPurposePreProcessCreateVolumeValidator";
    assertEquals(expectedMethodName, preFinalizeValidators.get(0).getName());
    assertEquals(expectedMethodName, newClientValidators.get(0).getName());
  }

  @Test
  public void testRegistryHasTheMultiPurposePostProcessCreateVolumeValidator() {
    ValidatorRegistry registry = new ValidatorRegistry(PACKAGE);
    List<Method> preFinalizeValidators =
        registry.validationsFor(
            asList(CLUSTER_NEEDS_FINALIZATION), CreateVolume, POST_PROCESS);
    List<Method> oldClientValidators =
        registry.validationsFor(
            asList(OLDER_CLIENT_REQUESTS), CreateVolume, POST_PROCESS);

    assertEquals(1, preFinalizeValidators.size());
    assertEquals(1, oldClientValidators.size());
    String expectedMethodName = "multiPurposePostProcessCreateVolumeValidator";
    assertEquals(expectedMethodName, preFinalizeValidators.get(0).getName());
    assertEquals(expectedMethodName, oldClientValidators.get(0).getName());
  }

  @Test
  public void testValidatorsAreReturnedForMultiCondition() {
    ValidatorRegistry registry = new ValidatorRegistry(PACKAGE);
    List<Method> validators =
        registry.validationsFor(
            asList(CLUSTER_NEEDS_FINALIZATION, OLDER_CLIENT_REQUESTS),
            CreateKey, POST_PROCESS);

    assertEquals(3, validators.size());
    List<String> methodNames =
        validators.stream().map(Method::getName).collect(Collectors.toList());
    assertThat(methodNames).contains("preFinalizePostProcessCreateKeyValidator");
    assertThat(methodNames).contains("oldClientPostProcessCreateKeyValidator");
    assertThat(methodNames).contains("oldClientPostProcessCreateKeyValidator2");
  }

  @Test
  public void testNoValidatorForRequestsAtAllReturnsEmptyList() {
    ValidatorRegistry registry = new ValidatorRegistry(PACKAGE_WO_VALIDATORS);

    assertTrue(registry.validationsFor(
        asList(OLDER_CLIENT_REQUESTS), CreateKey, PRE_PROCESS).isEmpty());
  }

  @Test
  public void testNoValidatorForConditionReturnsEmptyList()
      throws MalformedURLException {
    Collection<URL> urls = ClasspathHelper.forPackage(PACKAGE2);
    Collection<URL> urlsToUse = new ArrayList<>();
    for (URL url : urls) {
      urlsToUse.add(new URL(url, PACKAGE2.replaceAll("\\.", "/")));
    }
    ValidatorRegistry registry = new ValidatorRegistry(urlsToUse);

    assertTrue(registry.validationsFor(
        asList(CLUSTER_NEEDS_FINALIZATION), CreateKey, PRE_PROCESS).isEmpty());
  }

  @Test
  public void testNoDefinedValidationForRequestReturnsEmptyList() {
    ValidatorRegistry registry = new ValidatorRegistry(PACKAGE);

    assertTrue(registry.validationsFor(
        asList(OLDER_CLIENT_REQUESTS), CreateDirectory, null).isEmpty());
  }

}
