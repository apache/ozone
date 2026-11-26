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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.DN_LOGGER;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.OM_LOGGER;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.SCM_LOGGER;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.SET_LEVEL;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.assertGetLogLevelResponse;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.assertSetLogLevelResponse;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.getDnHttpAddress;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.getLogLevel;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.getOmHttpAddress;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.getScmHttpAddress;
import static org.apache.hadoop.ozone.om.LogLevelEndpointTestUtil.setLogLevel;

import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration tests for the /logLevel HTTP endpoint in a non-secure cluster.
 */
public class TestLogLevelEndpointInsecure {

  private static MiniOzoneCluster cluster;
  private static String omHttpAddress;
  private static String scmHttpAddress;
  private static String dnHttpAddress;

  @BeforeAll
  static void startCluster() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    UserGroupInformation.setConfiguration(
        LegacyHadoopConfigurationSource.asHadoopConfiguration(conf));
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    omHttpAddress = getOmHttpAddress(cluster);
    scmHttpAddress = getScmHttpAddress(cluster);
    dnHttpAddress = getDnHttpAddress(cluster);
  }

  @AfterAll
  static void stopCluster() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }

  @ParameterizedTest(name = "GET /logLevel on {0}")
  @MethodSource("httpEndpoints")
  void testGetLogLevel(String serviceName, String httpAddress, String logger)
      throws Exception {
    String response = getLogLevel(httpAddress, logger);
    assertGetLogLevelResponse(response, serviceName);
  }

  @ParameterizedTest(name = "SET /logLevel on {0}")
  @MethodSource("httpEndpoints")
  void testSetLogLevel(String serviceName, String httpAddress, String logger)
      throws Exception {
    String setResponse = setLogLevel(httpAddress, logger, SET_LEVEL);
    assertSetLogLevelResponse(setResponse, serviceName, SET_LEVEL);

    String getResponse = getLogLevel(httpAddress, logger);
    assertGetLogLevelResponse(getResponse, serviceName);
    LogLevelEndpointTestUtil.assertEffectiveLevel(getResponse, SET_LEVEL,
        serviceName);
  }

  static Stream<Arguments> httpEndpoints() {
    return Stream.of(
        Arguments.of("OM", omHttpAddress, OM_LOGGER),
        Arguments.of("SCM", scmHttpAddress, SCM_LOGGER),
        Arguments.of("DN", dnHttpAddress, DN_LOGGER));
  }
}
