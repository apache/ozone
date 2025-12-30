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

package org.apache.hadoop.hdds.fs;

import java.io.File;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configures disk space checks (du, df, etc.) for HDDS volumes, allowing
 * different implementations and parameters for different volumes.
 * Eg. if a volume has a dedicated disk, it can use the faster
 * df-based implementation.
 *
 * {@code SpaceUsageCheckFactory} implementations should have
 * a no-arg constructor for config-based instantiation.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface SpaceUsageCheckFactory {

  String CONFIG_PREFIX = "hdds.datanode.du.factory";

  /**
   * Creates configuration for the HDDS volume rooted at {@code dir}.
   *
   * @throws UncheckedIOException if canonical path for {@code dir} cannot be
   * resolved
   */
  SpaceUsageCheckParams paramsFor(File dir);

  /**
   * Creates configuration for the HDDS volume rooted at {@code dir} with exclusion path for du.
   *
   * @throws UncheckedIOException if canonical path for {@code dir} cannot be resolved
   */
  default SpaceUsageCheckParams paramsFor(File dir, Supplier<File> exclusionProvider) {
    return paramsFor(dir);
  }

  /**
   * Updates the factory with global configuration.
   * @return factory configured with {@code conf}
   */
  default SpaceUsageCheckFactory setConfiguration(ConfigurationSource conf) {
    // override if configurable
    return this;
  }

  /**
   * Creates a "global" implementation based on the class specified for
   * {@link Conf#setClassName(String)} in {@code conf}.
   * Defaults to {@link DUFactory} if no class is configured or it cannot be
   * instantiated.
   */
  static SpaceUsageCheckFactory create(ConfigurationSource config) {
    Conf conf = config.getObject(Conf.class);
    Class<? extends SpaceUsageCheckFactory> aClass = null;
    String className = conf.getClassName();
    if (className != null && !className.isEmpty()) {
      try {
        aClass =
            SpaceUsageCheckFactory.class
                .getClassLoader().loadClass(className)
                .asSubclass(SpaceUsageCheckFactory.class);
      } catch (ClassNotFoundException | RuntimeException e) {
        Logger log = LoggerFactory.getLogger(SpaceUsageCheckFactory.class);
        log.warn("Error trying to create SpaceUsageCheckFactory: '{}'",
            className, e);
      }
    }

    SpaceUsageCheckFactory instance = null;

    if (aClass != null) {
      try {
        Constructor<? extends SpaceUsageCheckFactory> constructor =
            aClass.getConstructor();
        instance = constructor.newInstance();
      } catch (IllegalAccessException | InstantiationException |
          InvocationTargetException | NoSuchMethodException |
          ClassCastException e) {

        Logger log = LoggerFactory.getLogger(SpaceUsageCheckFactory.class);
        log.warn("Error trying to create {}", aClass, e);
      }
    }

    if (instance == null) {
      instance = defaultImplementation();
    }

    return instance.setConfiguration(config);
  }

  static SpaceUsageCheckFactory defaultImplementation() {
    return new DUOptimizedFactory();
  }

  /**
   * Configuration for {@link SpaceUsageCheckFactory}.
   */
  @ConfigGroup(prefix = CONFIG_PREFIX)
  class Conf {

    private static final String CLASSNAME_KEY = "classname";

    @Config(
        key = "hdds.datanode.du.factory.classname",
        defaultValue = "",
        tags = { ConfigTag.DATANODE },
        description = "The fully qualified name of the factory class that "
            + "creates objects for providing disk space usage information.  It "
            + "should implement the SpaceUsageCheckFactory interface."
    )
    private String className;

    public void setClassName(String className) {
      this.className = className;
    }

    public String getClassName() {
      return className;
    }

    public static String configKeyForClassName() {
      return CONFIG_PREFIX + "." + CLASSNAME_KEY;
    }
  }

}
