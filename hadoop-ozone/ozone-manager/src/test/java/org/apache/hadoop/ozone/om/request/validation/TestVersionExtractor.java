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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.lang.annotation.Annotation;
import java.util.Map;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestVersionExtractor {

  @ParameterizedTest
  @EnumSource(OMLayoutFeature.class)
  void testLayoutVersionExtractor(OMLayoutFeature layoutVersionValue) throws OMException {
    ValidationContext context = mock(ValidationContext.class);
    LayoutVersionManager layoutVersionManager = new OMLayoutVersionManager(layoutVersionValue.serialize());
    when(context.versionManager()).thenReturn(layoutVersionManager);
    ComponentVersion version =
        VersionExtractor.LAYOUT_VERSION_EXTRACTOR.extractVersion(null, context);
    assertEquals(layoutVersionValue, version);
  }

  @ParameterizedTest
  @EnumSource(ClientVersion.class)
  void testClientVersionExtractor(ClientVersion expectedClientVersion) {
    OMRequest request = mock(OMRequest.class);
    when(request.getVersion()).thenReturn(expectedClientVersion.serialize());
    ComponentVersion version =
        VersionExtractor.CLIENT_VERSION_EXTRACTOR.extractVersion(request, null);
    assertEquals(expectedClientVersion, version);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 5, 10, 1000, 10000})
  void testClientVersionExtractorForFutureValues(int futureVersion) {
    OMRequest request = mock(OMRequest.class);
    when(request.getVersion()).thenReturn(ClientVersion.CURRENT.serialize() + futureVersion);
    ComponentVersion version =
        VersionExtractor.CLIENT_VERSION_EXTRACTOR.extractVersion(request, null);
    assertEquals(ClientVersion.FUTURE_VERSION, version);
  }

  @Test
  void testGetValidatorClass() {
    Map<VersionExtractor, Class<? extends Annotation>> expectedValidatorClasses =
        ImmutableMap.of(VersionExtractor.CLIENT_VERSION_EXTRACTOR, OMClientVersionValidator.class,
            VersionExtractor.LAYOUT_VERSION_EXTRACTOR, OMLayoutVersionValidator.class);
    for (VersionExtractor extractor : VersionExtractor.values()) {
      assertEquals(expectedValidatorClasses.get(extractor), extractor.getValidatorClass());
    }
  }
}
