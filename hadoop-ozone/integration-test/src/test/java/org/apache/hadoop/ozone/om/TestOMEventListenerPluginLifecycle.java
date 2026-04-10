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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;

import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.eventlistener.NoOpOMEventListener;
import org.apache.hadoop.ozone.om.eventlistener.OMEventListener;
import org.apache.hadoop.ozone.om.eventlistener.OMEventListenerPluginManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Integration test to verify OMEventListener plugin lifecycle within OzoneManager.
 */
@Timeout(300)
public class TestOMEventListenerPluginLifecycle {

  private static MiniOzoneCluster cluster = null;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);

    // Configure NoOpOMEventListener
    conf.set("ozone.om.plugin.destination.foo", "enabled");
    conf.set("ozone.om.plugin.destination.foo.classname", NoOpOMEventListener.class.getName());

    cluster = MiniOzoneCluster.newBuilder(conf)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testPluginLifecycleWithRestart() throws Exception {
    OzoneManager om = cluster.getOzoneManager();
    OMEventListenerPluginManager pluginManager = om.getEventListenerPluginManager();

    List<OMEventListener> plugins = pluginManager.getLoaded();
    Assertions.assertEquals(1, plugins.size(), "Should have 1 plugin loaded");

    NoOpOMEventListener plugin = (NoOpOMEventListener) plugins.get(0);

    Assertions.assertTrue(plugin.isInitialized(), "Plugin should be initialized");
    Assertions.assertTrue(plugin.isStarted(), "Plugin should be started");
    Assertions.assertFalse(plugin.isStopped(), "Plugin should not be stopped");

    // Restart OM to verify restart lifecycle
    cluster.restartOzoneManager();

    // Get the new OM instance and plugin
    om = cluster.getOzoneManager();
    pluginManager = om.getEventListenerPluginManager();

    plugins = pluginManager.getLoaded();
    Assertions.assertEquals(1, plugins.size(), "Should have 1 plugin loaded after restart");
    plugin = (NoOpOMEventListener) plugins.get(0);

    Assertions.assertTrue(plugin.isInitialized(), "Plugin should be initialized after restart");
    Assertions.assertTrue(plugin.isStarted(), "Plugin should be started after restart");
    Assertions.assertFalse(plugin.isStopped(), "Plugin should not be stopped after restart");
  }
}
