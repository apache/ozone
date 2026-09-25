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

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.http.HttpServer2;
import org.apache.hadoop.hdds.server.http.HttpServerConfigurationException;
import org.apache.hadoop.hdds.server.http.TestHttpServer2;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.jupiter.api.Test;

/**
 * Verifies that OM aborts start-up when its web server cannot be configured, instead of logging
 * the failure and continuing to serve without a web server. Both entry points are covered, since
 * either could stop calling the shared web-server start-up without any other test noticing.
 *
 * <p>Each test owns its cluster: the assertions leave OM deliberately half-started, which a
 * shared cluster could not recover from.
 */
public class TestOzoneManagerHttpServerStartup {

  /**
   * A javax filter that cannot be bridged into the Jakarta chain leaves the OM web server
   * unusable, and such a misconfiguration will never succeed on retry, so a booting OM must fail
   * fast rather than come up without a web server.
   */
  @Test
  public void startThrowsOnNonBridgeableFilter() throws Exception {
    MiniOzoneCluster cluster = newCluster();
    try {
      OzoneManager running = cluster.getOzoneManager();
      OzoneConfiguration omConf = running.getConfiguration();
      // Release the OM metadata DB and its ports so a fresh OM can boot from the same
      // configuration. SCM stays up, so only OM start-up is under test.
      stop(running);
      breakHttpFilterInitializer(omConf);

      OzoneManager booting = OzoneManager.createOm(omConf);
      try {
        assertThrows(HttpServerConfigurationException.class, booting::start);
      } finally {
        stop(booting);
      }
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * The same must hold on the restart path, which shares its web-server start-up with
   * {@link OzoneManager#start()}.
   */
  @Test
  public void restartThrowsOnNonBridgeableFilter() throws Exception {
    MiniOzoneCluster cluster = newCluster();
    try {
      OzoneManager om = cluster.getOzoneManager();
      stop(om);
      breakHttpFilterInitializer(om.getConfiguration());

      assertThrows(HttpServerConfigurationException.class, om::restart);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * The filter is configured only after the cluster is up: {@code ozone.http.filter.initializers}
   * is global, so setting it up front would abort SCM start-up first and never reach OM.
   */
  private static void breakHttpFilterInitializer(OzoneConfiguration conf) {
    conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
        TestHttpServer2.NonBridgeableFilterInitializer.class.getName());
  }

  private static MiniOzoneCluster newCluster() throws Exception {
    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(new OzoneConfiguration())
        .withoutDatanodes()
        .build();
    cluster.waitForClusterToBeReady();
    return cluster;
  }

  private static void stop(OzoneManager om) {
    if (om.stop()) {
      om.join();
    }
  }
}
