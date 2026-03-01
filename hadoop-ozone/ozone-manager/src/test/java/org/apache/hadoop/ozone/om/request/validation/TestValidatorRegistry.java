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

import static org.apache.hadoop.ozone.om.request.validation.VersionExtractor.CLIENT_VERSION_EXTRACTOR;
import static org.apache.hadoop.ozone.om.request.validation.VersionExtractor.LAYOUT_VERSION_EXTRACTOR;
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.Versioned;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
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

  private static final Set<RequestProcessingPhase> REQUEST_PROCESSING_PHASES =
      Sets.immutableEnumSet(PRE_PROCESS, POST_PROCESS);

  @BeforeEach
  public void setup() {
    startValidatorTest();
  }

  @AfterEach
  public void tearDown() {
    finishValidatorTest();
  }

  private List<Method> getValidatorsForRequest(OzoneManagerProtocolProtos.Type requestType,
      RequestProcessingPhase phase, VersionExtractor extractor, Versioned versioned) {
    return getValidatorsForRequest(PACKAGE, requestType, phase, Collections.singletonMap(extractor, versioned));
  }

  private List<Method> getValidatorsForRequest(OzoneManagerProtocolProtos.Type requestType,
      RequestProcessingPhase phase, Map<VersionExtractor, Versioned> requestVersions) {
    return getValidatorsForRequest(PACKAGE, requestType, phase, requestVersions);
  }

  private List<Method> getValidatorsForRequest(String packageName, OzoneManagerProtocolProtos.Type requestType,
      RequestProcessingPhase phase, Map<VersionExtractor, Versioned> requestVersions) {
    ValidatorRegistry<OzoneManagerProtocolProtos.Type> registry =
        new ValidatorRegistry<>(OzoneManagerProtocolProtos.Type.class, packageName,
            Arrays.stream(VersionExtractor.values()).map(VersionExtractor::getValidatorClass)
                .collect(Collectors.toSet()), REQUEST_PROCESSING_PHASES);
    return registry.validationsFor(requestType, phase, requestVersions.entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().getValidatorClass(), Map.Entry::getValue)));
  }

  @Test
  public void testNoValidatorsReturnedForEmptyConditionList() {
    List<Method> validators = getValidatorsForRequest(CreateKey, PRE_PROCESS,
        CLIENT_VERSION_EXTRACTOR, ClientVersion.CURRENT);
    assertTrue(validators.isEmpty());
  }

  @Test
  public void testRegistryHasThePreFinalizePreProcessCreateKeyValidator() {
    List<Method> validators = getValidatorsForRequest(CreateKey, PRE_PROCESS,
        ImmutableMap.of(CLIENT_VERSION_EXTRACTOR, ClientVersion.CURRENT,
            LAYOUT_VERSION_EXTRACTOR, OMLayoutFeature.FILESYSTEM_SNAPSHOT));

    assertEquals(1, validators.size());
    String expectedMethodName = "preProcessCreateKeyQuotaLayoutValidator";
    assertEquals(expectedMethodName, validators.get(0).getName());
  }

  @Test
  public void testRegistryHasThePreFinalizePostProcessCreateKeyValidator() {
    List<Method> validators = getValidatorsForRequest(CreateKey, POST_PROCESS,
        ImmutableMap.of(CLIENT_VERSION_EXTRACTOR, ClientVersion.CURRENT,
            LAYOUT_VERSION_EXTRACTOR, OMLayoutFeature.BUCKET_LAYOUT_SUPPORT));

    assertEquals(1, validators.size());
    String expectedMethodName = "postProcessCreateKeyQuotaLayoutValidator";
    assertEquals(expectedMethodName, validators.get(0).getName());
  }

  @Test
  public void testRegistryHasTheOldClientPreProcessCreateKeyValidator() {
    List<Method> validators = getValidatorsForRequest(CreateKey, PRE_PROCESS, CLIENT_VERSION_EXTRACTOR,
        ClientVersion.ERASURE_CODING_SUPPORT);

    assertEquals(2, validators.size());
    List<String> methodNames =
        validators.stream().map(Method::getName).collect(Collectors.toList());
    assertEquals(Arrays.asList("preProcessCreateKeyBucketLayoutClientValidator",
        "preProcessCreateKeyBucketLayoutClientValidator"), methodNames);
  }

  @Test
  public void testRegistryHasTheOldClientPostProcessCreateKeyValidator() {
    List<Method> validators = getValidatorsForRequest(CreateKey, POST_PROCESS,
        CLIENT_VERSION_EXTRACTOR, ClientVersion.ERASURE_CODING_SUPPORT);

    assertEquals(1, validators.size());
    List<String> methodNames =
        validators.stream().map(Method::getName).collect(Collectors.toList());
    assertThat(methodNames).contains("postProcessCreateKeyBucketLayoutClientValidator");
  }

  @Test
  public void testRegistryHasTheMultiPurposePreProcessCreateVolumeValidator() {
    List<Method> preFinalizeValidators = getValidatorsForRequest(CreateVolume, PRE_PROCESS, LAYOUT_VERSION_EXTRACTOR,
        OMLayoutFeature.HSYNC);
    List<Method> newClientValidators = getValidatorsForRequest(CreateVolume, PRE_PROCESS, CLIENT_VERSION_EXTRACTOR,
            ClientVersion.ERASURE_CODING_SUPPORT);

    assertEquals(1, preFinalizeValidators.size());
    assertEquals(1, newClientValidators.size());
    String expectedMethodName = "multiPurposePreProcessCreateVolumeBucketLayoutCLientQuotaLayoutValidator";
    assertEquals(expectedMethodName, preFinalizeValidators.get(0).getName());
    assertEquals(expectedMethodName, newClientValidators.get(0).getName());
  }

  @Test
  public void testRegistryHasTheMultiPurposePostProcessCreateVolumeValidator() {
    List<Method> preFinalizeValidators = getValidatorsForRequest(CreateVolume, POST_PROCESS, LAYOUT_VERSION_EXTRACTOR,
        OMLayoutFeature.HSYNC);
    List<Method> oldClientValidators = getValidatorsForRequest(CreateVolume, POST_PROCESS, CLIENT_VERSION_EXTRACTOR,
        ClientVersion.ERASURE_CODING_SUPPORT);
    List<Method> combinedValidators = getValidatorsForRequest(CreateVolume, POST_PROCESS,
        ImmutableMap.of(LAYOUT_VERSION_EXTRACTOR, OMLayoutFeature.HSYNC,
            CLIENT_VERSION_EXTRACTOR, ClientVersion.ERASURE_CODING_SUPPORT));

    assertEquals(1, preFinalizeValidators.size());
    assertEquals(1, oldClientValidators.size());
    assertEquals(1, combinedValidators.size());
    String expectedMethodName = "multiPurposePostProcessCreateVolumeBucketLayoutCLientQuotaLayoutValidator";
    assertEquals(expectedMethodName, preFinalizeValidators.get(0).getName());
    assertEquals(expectedMethodName, oldClientValidators.get(0).getName());
    assertEquals(expectedMethodName, combinedValidators.get(0).getName());
  }

  @Test
  public void testValidatorsAreReturnedForMultiCondition() {
    List<Method> validators = getValidatorsForRequest(CreateKey, POST_PROCESS,
        ImmutableMap.of(CLIENT_VERSION_EXTRACTOR, ClientVersion.ERASURE_CODING_SUPPORT,
            LAYOUT_VERSION_EXTRACTOR, OMLayoutFeature.HSYNC));

    assertEquals(2, validators.size());
    List<String> methodNames =
        validators.stream().map(Method::getName).collect(Collectors.toList());
    assertThat(methodNames).contains("postProcessCreateKeyQuotaLayoutValidator");
    assertThat(methodNames).contains("postProcessCreateKeyBucketLayoutClientValidator");
  }

  @Test
  public void testNoValidatorForRequestsAtAllReturnsEmptyList() {
    assertTrue(getValidatorsForRequest(PACKAGE_WO_VALIDATORS, CreateKey, PRE_PROCESS,
        ImmutableMap.of(CLIENT_VERSION_EXTRACTOR, ClientVersion.ERASURE_CODING_SUPPORT,
            LAYOUT_VERSION_EXTRACTOR, OMLayoutFeature.HSYNC)).isEmpty());
  }

  @Test
  public void testNoValidatorForConditionReturnsEmptyList()
      throws MalformedURLException {
    Collection<URL> urls = ClasspathHelper.forPackage(PACKAGE2);
    Collection<URL> urlsToUse = new ArrayList<>();
    for (URL url : urls) {
      urlsToUse.add(new URL(url, PACKAGE2.replaceAll("\\.", "/")));
    }
    ValidatorRegistry<OzoneManagerProtocolProtos.Type> registry = new ValidatorRegistry<>(
        OzoneManagerProtocolProtos.Type.class, urlsToUse,
        Arrays.stream(VersionExtractor.values()).map(VersionExtractor::getValidatorClass).collect(Collectors.toSet()),
        REQUEST_PROCESSING_PHASES);

    assertTrue(registry.validationsFor(CreateKey, PRE_PROCESS,
        ImmutableMap.of(CLIENT_VERSION_EXTRACTOR.getValidatorClass(), ClientVersion.CURRENT,
            LAYOUT_VERSION_EXTRACTOR.getValidatorClass(), OMLayoutFeature.BUCKET_LAYOUT_SUPPORT)).isEmpty());
  }

  @Test
  public void testNoDefinedValidationForRequestReturnsEmptyList() {
    assertTrue(getValidatorsForRequest(CreateDirectory, null, CLIENT_VERSION_EXTRACTOR,
        ClientVersion.ERASURE_CODING_SUPPORT).isEmpty());
  }

  @Test
  public void testFutureVersionForRequestReturnsOnlyFutureVersionValidators() {
    List<Method> validators = getValidatorsForRequest(CreateKey, PRE_PROCESS,
        CLIENT_VERSION_EXTRACTOR, ClientVersion.FUTURE_VERSION);

    assertEquals(1, validators.size());
    List<String> methodNames =
        validators.stream().map(Method::getName).collect(Collectors.toList());
    assertThat(methodNames).contains("preProcessCreateKeyFutureClientValidator");
  }

}
