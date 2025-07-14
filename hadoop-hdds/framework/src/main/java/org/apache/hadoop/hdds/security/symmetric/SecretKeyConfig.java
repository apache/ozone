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

package org.apache.hadoop.hdds.security.symmetric;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_KEY_DIR_NAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_KEY_DIR_NAME_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_METADATA_DIR_NAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_ALGORITHM;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_ALGORITHM_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_EXPIRY_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_EXPIRY_DURATION_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_FILE;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_FILE_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_ROTATE_CHECK_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_ROTATE_CHECK_DURATION_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_ROTATE_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_ROTATE_DURATION_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.ConfigurationSource;

/**
 * Configurations related to SecretKeys lifecycle management.
 */
public class SecretKeyConfig {
  private final Path localSecretKeyFile;
  private final Duration rotateDuration;
  private final Duration expiryDuration;
  private final String algorithm;
  private final Duration rotationCheckDuration;

  public SecretKeyConfig(ConfigurationSource conf, String component) {
    String metadataDir = conf.get(HDDS_METADATA_DIR_NAME,
        conf.get(OZONE_METADATA_DIRS));
    String keyDir = conf.get(HDDS_KEY_DIR_NAME, HDDS_KEY_DIR_NAME_DEFAULT);
    String fileName = conf.get(HDDS_SECRET_KEY_FILE,
        HDDS_SECRET_KEY_FILE_DEFAULT);
    localSecretKeyFile = Paths.get(metadataDir, component, keyDir, fileName);

    this.rotateDuration = parseRotateDuration(conf);
    this.expiryDuration = parseExpiryDuration(conf);
    this.rotationCheckDuration = parseRotateCheckDuration(conf);

    this.algorithm = conf.get(HDDS_SECRET_KEY_ALGORITHM,
        HDDS_SECRET_KEY_ALGORITHM_DEFAULT);
  }

  public static Duration parseExpiryDuration(ConfigurationSource conf) {
    long expiryDurationInMs = conf.getTimeDuration(
        HDDS_SECRET_KEY_EXPIRY_DURATION,
        HDDS_SECRET_KEY_EXPIRY_DURATION_DEFAULT, TimeUnit.MILLISECONDS);
    return Duration.ofMillis(expiryDurationInMs);
  }

  public static Duration parseRotateDuration(ConfigurationSource conf) {
    long rotateDurationInMs = conf.getTimeDuration(
        HDDS_SECRET_KEY_ROTATE_DURATION,
        HDDS_SECRET_KEY_ROTATE_DURATION_DEFAULT, TimeUnit.MILLISECONDS);
    return Duration.ofMillis(rotateDurationInMs);
  }

  public static Duration parseRotateCheckDuration(ConfigurationSource conf) {
    long rotationCheckInMs = conf.getTimeDuration(
        HDDS_SECRET_KEY_ROTATE_CHECK_DURATION,
        HDDS_SECRET_KEY_ROTATE_CHECK_DURATION_DEFAULT, TimeUnit.MILLISECONDS);
    return Duration.ofMillis(rotationCheckInMs);
  }

  public Path getLocalSecretKeyFile() {
    return localSecretKeyFile;
  }

  public Duration getRotateDuration() {
    return rotateDuration;
  }

  public Duration getExpiryDuration() {
    return expiryDuration;
  }

  public String getAlgorithm() {
    return algorithm;
  }

  public Duration getRotationCheckDuration() {
    return rotationCheckDuration;
  }
}
