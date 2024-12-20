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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.request.validation.ValidatorRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reflections.util.ClasspathHelper;

import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.request.validation.VersionExtractor.CLIENT_VERSION_EXTRACTOR;
import static org.apache.hadoop.ozone.om.request.validation.VersionExtractor.LAYOUT_VERSION_EXTRACTOR;
import static org.apache.hadoop.ozone.request.validation.RequestProcessingPhase.POST_PROCESS;
import static org.apache.hadoop.ozone.request.validation.RequestProcessingPhase.PRE_PROCESS;
import static org.apache.hadoop.ozone.om.request.validation.testvalidatorset1.GeneralValidatorsForTesting.startValidatorTest;
import static org.apache.hadoop.ozone.om.request.validation.testvalidatorset1.GeneralValidatorsForTesting.finishValidatorTest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateDirectory;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateKey;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateVolume;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  @Test
  public void testNoValidatorsReturnedForEmptyConditionList() {
    ValidatorRegistry<OzoneManagerProtocolProtos.Type> registry =
        new ValidatorRegistry<>(OzoneManagerProtocolProtos.Type.class, PACKAGE,
            Arrays.stream(VersionExtractor.values()).map(VersionExtractor::getValidatorClass)
                .collect(Collectors.toSet()), REQUEST_PROCESSING_PHASES);
    List<Method> validators = registry.validationsFor(CreateKey, PRE_PROCESS,
        CLIENT_VERSION_EXTRACTOR.getValidatorClass(), ClientVersion.CURRENT);

    assertTrue(validators.isEmpty());
  }

  @Test
  public void testRegistryHasThePreFinalizePreProcessCreateKeyValidator() {
    ValidatorRegistry<OzoneManagerProtocolProtos.Type> registry = new ValidatorRegistry<>(
        OzoneManagerProtocolProtos.Type.class, PACKAGE,
        Arrays.stream(VersionExtractor.values()).map(VersionExtractor::getValidatorClass).collect(Collectors.toSet()),
        REQUEST_PROCESSING_PHASES);
    List<Method> validators =
        registry.validationsFor(CreateKey, PRE_PROCESS,
            ImmutableMap.of(CLIENT_VERSION_EXTRACTOR.getValidatorClass(), ClientVersion.CURRENT,
                LAYOUT_VERSION_EXTRACTOR.getValidatorClass(), OMLayoutFeature.FILESYSTEM_SNAPSHOT));

    assertEquals(1, validators.size());
    String expectedMethodName = "preProcessCreateKeyQuotaLayoutValidator";
    assertEquals(expectedMethodName, validators.get(0).getName());
  }

  @Test
  public void testRegistryHasThePreFinalizePostProcessCreateKeyValidator() {
    ValidatorRegistry<OzoneManagerProtocolProtos.Type> registry =
        new ValidatorRegistry<>(OzoneManagerProtocolProtos.Type.class, PACKAGE,
        Arrays.stream(VersionExtractor.values()).map(VersionExtractor::getValidatorClass).collect(Collectors.toSet()),
        REQUEST_PROCESSING_PHASES);
    List<Method> validators = registry.validationsFor(CreateKey, POST_PROCESS,
        ImmutableMap.of(CLIENT_VERSION_EXTRACTOR.getValidatorClass(), ClientVersion.CURRENT,
            LAYOUT_VERSION_EXTRACTOR.getValidatorClass(), OMLayoutFeature.BUCKET_LAYOUT_SUPPORT));

    assertEquals(1, validators.size());
    String expectedMethodName = "postProcessCreateKeyQuotaLayoutValidator";
    assertEquals(expectedMethodName, validators.get(0).getName());
  }

  @Test
  public void testRegistryHasTheOldClientPreProcessCreateKeyValidator() {
    ValidatorRegistry<OzoneManagerProtocolProtos.Type> registry =
        new ValidatorRegistry<>(OzoneManagerProtocolProtos.Type.class, PACKAGE,
        Arrays.stream(VersionExtractor.values()).map(VersionExtractor::getValidatorClass).collect(Collectors.toSet()),
        REQUEST_PROCESSING_PHASES);
    List<Method> validators =
        registry.validationsFor(CreateKey, PRE_PROCESS, CLIENT_VERSION_EXTRACTOR.getValidatorClass(),
            ClientVersion.ERASURE_CODING_SUPPORT);

    assertEquals(2, validators.size());
    List<String> methodNames =
        validators.stream().map(Method::getName).collect(Collectors.toList());
    assertEquals(Arrays.asList("preProcessCreateKeyBucketLayoutClientValidator",
        "preProcessCreateKeyBucketLayoutClientValidator"), methodNames);
  }

  @Test
  public void testRegistryHasTheOldClientPostProcessCreateKeyValidator() {
    ValidatorRegistry<OzoneManagerProtocolProtos.Type> registry =
        new ValidatorRegistry<>(OzoneManagerProtocolProtos.Type.class, PACKAGE,
        Arrays.stream(VersionExtractor.values()).map(VersionExtractor::getValidatorClass).collect(Collectors.toSet()),
        REQUEST_PROCESSING_PHASES);
    List<Method> validators = registry.validationsFor(CreateKey, POST_PROCESS,
        CLIENT_VERSION_EXTRACTOR.getValidatorClass(), ClientVersion.ERASURE_CODING_SUPPORT);

    assertEquals(2, validators.size());
    List<String> methodNames =
        validators.stream().map(Method::getName).collect(Collectors.toList());
    assertThat(methodNames).contains("postProcessCreateKeyBucketLayoutClientValidator");
    assertThat(methodNames).contains("postProcessCreateKeyECReplicaIndexRequiredClientValidator");
  }

  @Test
  public void testRegistryHasTheMultiPurposePreProcessCreateVolumeValidator() {
    ValidatorRegistry<OzoneManagerProtocolProtos.Type> registry =
        new ValidatorRegistry<>(OzoneManagerProtocolProtos.Type.class, PACKAGE,
        Arrays.stream(VersionExtractor.values()).map(VersionExtractor::getValidatorClass).collect(Collectors.toSet()),
        REQUEST_PROCESSING_PHASES);
    List<Method> preFinalizeValidators =
        registry.validationsFor(CreateVolume, PRE_PROCESS, LAYOUT_VERSION_EXTRACTOR.getValidatorClass(),
            OMLayoutFeature.HSYNC);
    List<Method> newClientValidators =
        registry.validationsFor(CreateVolume, PRE_PROCESS, CLIENT_VERSION_EXTRACTOR.getValidatorClass(),
            ClientVersion.ERASURE_CODING_SUPPORT);

    assertEquals(1, preFinalizeValidators.size());
    assertEquals(1, newClientValidators.size());
    String expectedMethodName = "multiPurposePreProcessCreateVolumeBucketLayoutCLientQuotaLayoutValidator";
    assertEquals(expectedMethodName, preFinalizeValidators.get(0).getName());
    assertEquals(expectedMethodName, newClientValidators.get(0).getName());
  }

  @Test
  public void testRegistryHasTheMultiPurposePostProcessCreateVolumeValidator() {
    ValidatorRegistry<OzoneManagerProtocolProtos.Type> registry = new ValidatorRegistry<>(
        OzoneManagerProtocolProtos.Type.class, PACKAGE,
        Arrays.stream(VersionExtractor.values()).map(VersionExtractor::getValidatorClass).collect(Collectors.toSet()),
        REQUEST_PROCESSING_PHASES);
    List<Method> preFinalizeValidators =
        registry.validationsFor(CreateVolume, POST_PROCESS, LAYOUT_VERSION_EXTRACTOR.getValidatorClass(),
            OMLayoutFeature.HSYNC);
    List<Method> oldClientValidators =
        registry.validationsFor(CreateVolume, POST_PROCESS, CLIENT_VERSION_EXTRACTOR.getValidatorClass(),
            ClientVersion.ERASURE_CODING_SUPPORT);

    assertEquals(1, preFinalizeValidators.size());
    assertEquals(1, oldClientValidators.size());
    String expectedMethodName = "multiPurposePostProcessCreateVolumeBucketLayoutCLientQuotaLayoutValidator";
    assertEquals(expectedMethodName, preFinalizeValidators.get(0).getName());
    assertEquals(expectedMethodName, oldClientValidators.get(0).getName());
  }

  @Test
  public void testValidatorsAreReturnedForMultiCondition() {
    ValidatorRegistry<OzoneManagerProtocolProtos.Type> registry = new ValidatorRegistry<>(
        OzoneManagerProtocolProtos.Type.class, PACKAGE,
        Arrays.stream(VersionExtractor.values()).map(VersionExtractor::getValidatorClass).collect(Collectors.toSet()),
        REQUEST_PROCESSING_PHASES);
    List<Method> validators = registry.validationsFor(CreateKey, POST_PROCESS,
            ImmutableMap.of(CLIENT_VERSION_EXTRACTOR.getValidatorClass(), ClientVersion.ERASURE_CODING_SUPPORT,
                LAYOUT_VERSION_EXTRACTOR.getValidatorClass(), OMLayoutFeature.HSYNC));

    assertEquals(3, validators.size());
    List<String> methodNames =
        validators.stream().map(Method::getName).collect(Collectors.toList());
    assertThat(methodNames).contains("postProcessCreateKeyQuotaLayoutValidator");
    assertThat(methodNames).contains("postProcessCreateKeyBucketLayoutClientValidator");
    assertThat(methodNames).contains("postProcessCreateKeyECReplicaIndexRequiredClientValidator");
  }

  @Test
  public void testNoValidatorForRequestsAtAllReturnsEmptyList() {

    ValidatorRegistry<OzoneManagerProtocolProtos.Type> registry = new ValidatorRegistry<>(
        OzoneManagerProtocolProtos.Type.class, PACKAGE_WO_VALIDATORS,
        Arrays.stream(VersionExtractor.values()).map(VersionExtractor::getValidatorClass).collect(Collectors.toSet()),
        REQUEST_PROCESSING_PHASES);
    assertTrue(registry.validationsFor(CreateKey, PRE_PROCESS,
        ImmutableMap.of(CLIENT_VERSION_EXTRACTOR.getValidatorClass(), ClientVersion.ERASURE_CODING_SUPPORT,
            LAYOUT_VERSION_EXTRACTOR.getValidatorClass(), OMLayoutFeature.HSYNC)).isEmpty());
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
    ValidatorRegistry<OzoneManagerProtocolProtos.Type> registry = new ValidatorRegistry<>(
        OzoneManagerProtocolProtos.Type.class, PACKAGE,
        Arrays.stream(VersionExtractor.values()).map(VersionExtractor::getValidatorClass).collect(Collectors.toSet()),
        REQUEST_PROCESSING_PHASES);

    assertTrue(registry.validationsFor(CreateDirectory, null, CLIENT_VERSION_EXTRACTOR.getValidatorClass(),
        ClientVersion.ERASURE_CODING_SUPPORT).isEmpty());
  }

  @Test
  public void testFutureVersionForRequestReturnsOnlyFutureVersionValidators() {
    ValidatorRegistry<OzoneManagerProtocolProtos.Type> registry = new ValidatorRegistry<>(
        OzoneManagerProtocolProtos.Type.class, PACKAGE,
        Arrays.stream(VersionExtractor.values()).map(VersionExtractor::getValidatorClass).collect(Collectors.toSet()),
        REQUEST_PROCESSING_PHASES);

    List<Method> validators = registry.validationsFor(CreateKey, PRE_PROCESS,
        CLIENT_VERSION_EXTRACTOR.getValidatorClass(), ClientVersion.FUTURE_VERSION);

    assertEquals(1, validators.size());
    List<String> methodNames =
        validators.stream().map(Method::getName).collect(Collectors.toList());
    assertThat(methodNames).contains("preProcessCreateKeyFutureClientValidator");
  }

}
