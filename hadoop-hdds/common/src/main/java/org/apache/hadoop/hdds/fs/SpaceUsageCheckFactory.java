/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

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

  /**
   * Creates configuration for the HDDS volume rooted at {@code dir}.  Can use
   * implementation-specific settings from {@code conf}.
   *
   * @throws UncheckedIOException if canonical path for {@code dir} cannot be
   * resolved
   */
  SpaceUsageCheckParams paramsFor(Configuration conf, File dir);

  /**
   * Creates a "global" implementation based on the class specified for
   * {@link HddsConfigKeys#HDDS_DU_FACTORY_CLASS_KEY} in {@code conf}.
   * Defaults to {@link DUFactory} if no class is configured or it cannot be
   * instantiated.
   */
  static SpaceUsageCheckFactory create(Configuration conf) {
    Class<? extends SpaceUsageCheckFactory> aClass;
    try {
      aClass = conf.getClass(
          HddsConfigKeys.HDDS_DU_FACTORY_CLASS_KEY, null,
          SpaceUsageCheckFactory.class);
    } catch (RuntimeException e) {
      Logger log = LoggerFactory.getLogger(SpaceUsageCheckFactory.class);
      String className = conf.get(HddsConfigKeys.HDDS_DU_FACTORY_CLASS_KEY);
      log.warn("Error trying to create SpaceUsageCheckFactory: '{}'",
          className, e);

      return defaultFactory();
    }

    try {
      Constructor<? extends SpaceUsageCheckFactory> constructor =
          aClass.getConstructor();
      return constructor.newInstance();
    } catch (IllegalAccessException | InstantiationException |
        InvocationTargetException | NoSuchMethodException e) {

      Logger log = LoggerFactory.getLogger(SpaceUsageCheckFactory.class);
      log.warn("Error trying to create {}", aClass, e);

      return defaultFactory();
    }
  }

  static DUFactory defaultFactory() {
    return new DUFactory();
  }

}
