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

import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.Version;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestVersionExtractor {

  private static Stream<Arguments> layoutValues() {
    return Arrays.stream(OMLayoutFeature.values()).map(Arguments::of);
  }

  private static Stream<Arguments> clientVersionValues() {
    return Stream.of(
        Arrays.stream(ClientVersion.values())
            .map(clientVersion -> Arguments.of(clientVersion, clientVersion.version())),
        IntStream.range(1, 10)
            .mapToObj(delta -> Arguments.of(ClientVersion.FUTURE_VERSION, ClientVersion.CURRENT_VERSION + delta))
        ).flatMap(Function.identity());
  }

  @ParameterizedTest
  @MethodSource("layoutValues")
  void testLayoutVersionExtractor(OMLayoutFeature layoutVersionValue) throws OMException {
    ValidationContext context = mock(ValidationContext.class);
    LayoutVersionManager layoutVersionManager = new OMLayoutVersionManager(layoutVersionValue.version());
    when(context.versionManager()).thenReturn(layoutVersionManager);
    Version version = VersionExtractor.LAYOUT_VERSION_EXTRACTOR.extractVersion(null, context);
    assertEquals(layoutVersionValue, version);
    assertEquals(OMLayoutFeature.class, VersionExtractor.LAYOUT_VERSION_EXTRACTOR.getVersionClass());
  }

  @ParameterizedTest
  @MethodSource("clientVersionValues")
  void testClientVersionExtractor(ClientVersion expectedClientVersion, int clientVersion) {
    OMRequest request = mock(OMRequest.class);
    when(request.getVersion()).thenReturn(clientVersion);
    Version version = VersionExtractor.CLIENT_VERSION_EXTRACTOR.extractVersion(request, null);
    assertEquals(expectedClientVersion, version);
    assertEquals(ClientVersion.class, VersionExtractor.CLIENT_VERSION_EXTRACTOR.getVersionClass());
  }
}
