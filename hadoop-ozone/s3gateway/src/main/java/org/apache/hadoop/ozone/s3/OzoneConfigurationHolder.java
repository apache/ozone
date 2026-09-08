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

package org.apache.hadoop.ozone.s3;

import javax.enterprise.inject.Produces;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * Ozone Configuration factory.
 * <p>
 * As the OzoneConfiguration is created by the CLI application here we inject
 * it via a singleton instance to the Jax-RS/CDI instances.
 */
public final class OzoneConfigurationHolder {

  private static OzoneConfiguration configuration;

  private OzoneConfigurationHolder() {
  }

  @Produces
  public static OzoneConfiguration configuration() {
    return configuration;
  }

  public static void setConfiguration(
      OzoneConfiguration conf) {
    // First writer wins: S3GatewayService and LocalOzoneCluster set this before Gateway#call()
    // reaches it, and their configuration is the one the in-JVM gateway has to run with.
    if (configuration == null) {
      OzoneConfigurationHolder.configuration = conf;
    }
  }

  public static void resetConfiguration() {
    configuration = null;
  }
}
