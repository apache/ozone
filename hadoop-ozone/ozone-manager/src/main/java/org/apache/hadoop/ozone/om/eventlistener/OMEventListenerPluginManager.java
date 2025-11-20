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
package org.apache.hadoop.ozone.om.eventlistener;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

/**
 * This is a manager for plugins which implement OMEventListener which
 * manages the lifecycle of constructing starting/stopping configured
 * plugins.
 */
public class OMEventListenerPluginManager {
  public static final Logger LOG = LoggerFactory.getLogger(OMEventListenerPluginManager.class);

  public static final String PLUGIN_DEST_BASE = "ozone.om.plugin.destination";

  private final List<OMEventListener> plugins;

  public OMEventListenerPluginManager(OzoneManager ozoneManager, OzoneConfiguration conf) {
    this.plugins = loadAll(ozoneManager, conf);
  }

  public List<OMEventListener> getLoaded() {
    return plugins;
  }

  public void startAll() {
    for (OMEventListener plugin : plugins) {
      plugin.start();
    }
  }

  public void shutdownAll() {
    for (OMEventListener plugin : plugins) {
      plugin.shutdown();
    }
  }

  // Configuration is based on ranger plugins
  //
  // For example, a plugin named FooPlugin would be configured via
  // OzoneConfiguration properties as follows:
  //
  //   conf.set("ozone.om.plugin.destination.foo", "enabled");
  //   conf.set("ozone.om.plugin.destination.foo.classname", "org.apache.hadoop.ozone.om.eventlistener.FooPlugin");
  //
  static List<OMEventListener> loadAll(OzoneManager ozoneManager, OzoneConfiguration conf) {
    List<OMEventListener> plugins = new ArrayList<>();

    Map<String, String> props = conf.getPropsMatchPrefixAndTrimPrefix(PLUGIN_DEST_BASE);
    List<String> destNameList = new ArrayList<>();
    for (Map.Entry<String, String> entry : props.entrySet()) {
      String destName = entry.getKey();
      String value = entry.getValue();
      LOG.info("Found event listener plugin with name={} and value={}", destName, value);

      if (value.equalsIgnoreCase("enable") || value.equalsIgnoreCase("enabled") || value.equalsIgnoreCase("true")) {
        destNameList.add(destName);
        LOG.info("Event listener plugin {}{} is set to {}", PLUGIN_DEST_BASE, destName, value);
      }
    }

    OMEventListenerPluginContext pluginContext = new OMEventListenerPluginContextImpl(ozoneManager);

    for (String destName : destNameList) {
      try {
        Class<? extends OMEventListener> cls = resolvePluginClass(conf, destName);
        LOG.info("Event listener plugin class is {}", cls);

        OMEventListener impl = cls.newInstance();
        impl.initialize(conf, pluginContext);

        plugins.add(impl);
      } catch (Exception ex) {
        LOG.error("Can't make instance of event listener plugin {}{}", PLUGIN_DEST_BASE, destName, ex);
      }
    }

    return plugins;
  }

  private static Class<? extends OMEventListener> resolvePluginClass(OzoneConfiguration conf,
                                                                     String destName) {
    String classnameProp = PLUGIN_DEST_BASE + destName + ".classname";
    LOG.info("Gettting classname for {} with propety {}", destName, classnameProp);
    Class<? extends OMEventListener> cls = conf.getClass(classnameProp, null, OMEventListener.class);
    if (null == cls) {
      throw new RuntimeException(String.format(
          "Unable to load plugin %s, classname property %s is missing or does not implement OMEventListener",
          destName, classnameProp));
    }
    return cls;
  }
}
