/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.eventlistener;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link OMEventListenerPluginManager}.
 */
@ExtendWith(MockitoExtension.class)
public class TestOMEventListenerPluginManager {

  @Mock
  private OzoneManager ozoneManager;

  static List<String> getLoadedPlugins(OMEventListenerPluginManager pluginManager) {
    List<String> loadedClasses = new ArrayList<>();
    for (OMEventListener plugin : pluginManager.getLoaded()) {
      loadedClasses.add(plugin.getClass().getName());
    }

    // normalize
    Collections.sort(loadedClasses);

    return loadedClasses;
  }

  private static class BrokenFooPlugin {

  }

  @Test
  public void testLoadSinglePlugin() throws InterruptedException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.om.plugin.destination.foo", "enabled");
    conf.set("ozone.om.plugin.destination.foo.classname", "org.apache.hadoop.ozone.om.eventlistener.FooPlugin");

    OMEventListenerPluginManager pluginManager = new OMEventListenerPluginManager(ozoneManager, conf);

    Assertions.assertEquals(Arrays.asList("org.apache.hadoop.ozone.om.eventlistener.FooPlugin"),
                            getLoadedPlugins(pluginManager));
  }

  @Test
  public void testLoadMultiplePlugins() throws InterruptedException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.om.plugin.destination.foo", "enabled");
    conf.set("ozone.om.plugin.destination.foo.classname", "org.apache.hadoop.ozone.om.eventlistener.FooPlugin");
    conf.set("ozone.om.plugin.destination.bar", "enabled");
    conf.set("ozone.om.plugin.destination.bar.classname", "org.apache.hadoop.ozone.om.eventlistener.BarPlugin");

    OMEventListenerPluginManager pluginManager = new OMEventListenerPluginManager(ozoneManager, conf);

    Assertions.assertEquals(Arrays.asList("org.apache.hadoop.ozone.om.eventlistener.BarPlugin",
                                          "org.apache.hadoop.ozone.om.eventlistener.FooPlugin"),

                            getLoadedPlugins(pluginManager));
  }

  @Test
  public void testPluginMissingClassname() throws InterruptedException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.om.plugin.destination.foo", "enabled");

    OMEventListenerPluginManager pluginManager = new OMEventListenerPluginManager(ozoneManager, conf);

    Assertions.assertEquals(Arrays.asList(),
                            getLoadedPlugins(pluginManager));
  }

  @Test
  public void testPluginClassDoesNotExist() throws InterruptedException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.om.plugin.destination.foo", "enabled");
    conf.set("ozone.om.plugin.destination.foo.classname", "org.apache.hadoop.ozone.om.eventlistener.NotExistingPlugin");

    OMEventListenerPluginManager pluginManager = new OMEventListenerPluginManager(ozoneManager, conf);

    Assertions.assertEquals(Arrays.asList(),
                            getLoadedPlugins(pluginManager));
  }

  @Test
  public void testPluginClassDoesNotImplementInterface() throws InterruptedException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.om.plugin.destination.foo", "enabled");
    conf.set("ozone.om.plugin.destination.foo.classname", "org.apache.hadoop.ozone.om.eventlistener.BrokenFooPlugin");

    OMEventListenerPluginManager pluginManager = new OMEventListenerPluginManager(ozoneManager, conf);

    Assertions.assertEquals(Arrays.asList(),
                            getLoadedPlugins(pluginManager));
  }
}
