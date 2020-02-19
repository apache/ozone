/*
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
package org.apache.hadoop.ozone.recon;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Provider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * Ozone Configuration Provider.
 * <p>
 * As the OzoneConfiguration is created by the CLI application here we inject
 * it via a singleton instance to the Jax-RS/CDI instances.
 */
public class ConfigurationProvider implements
    Provider<OzoneConfiguration> {

  private static OzoneConfiguration configuration;

  @VisibleForTesting
  public static void setConfiguration(OzoneConfiguration conf) {
    if (configuration == null) {
      ConfigurationProvider.configuration = conf;
    }
  }

  @VisibleForTesting
  public static void resetConfiguration() {
    configuration = null;
  }

  @Override
  public OzoneConfiguration get() {
    return configuration;
  }
}
