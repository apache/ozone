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

package org.apache.hadoop.hdds.conf;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Example configuration to test the configuration injection.
 */
@ConfigGroup(prefix = "ozone.test.config")
public class ConfigurationExample extends ReconfigurableConfig {

  @Config(key = "ozone.test.config.address", defaultValue = "localhost", description = "Client "
      + "address (To test string injection).", tags = ConfigTag.MANAGEMENT)
  private String clientAddress;

  @Config(key = "ozone.test.config.bind.host", defaultValue = "0.0.0.0", description = "Bind "
      + "host(To test string injection).", tags = ConfigTag.MANAGEMENT)
  private String bindHost;

  @Config(key = "ozone.test.config.compression.enabled", defaultValue = "true", description =
      "Compression enabled. (Just to test boolean flag)", tags =
      ConfigTag.MANAGEMENT)
  private boolean compressionEnabled;

  @Config(key = "ozone.test.config.port", defaultValue = "1234", description = "Port number "
      + "config (To test in injection)", tags = ConfigTag.MANAGEMENT)
  private int port = 1234;

  @Config(key = "ozone.test.config.wait", type = ConfigType.TIME, timeUnit =
      TimeUnit.SECONDS, defaultValue = "30m", description = "Wait time (To "
      + "test TIME config type)", tags = ConfigTag.MANAGEMENT)
  private long waitTime = 1;

  @Config(key = "ozone.test.config.time.duration", type = ConfigType.TIME, timeUnit =
      TimeUnit.MINUTES, defaultValue = "1h", description = "N/A",
      tags = ConfigTag.MANAGEMENT)
  private Duration duration = Duration.ofSeconds(5);

  @Config(key = "ozone.test.config.size.small", type = ConfigType.SIZE, defaultValue = "42MB",
      tags = {},
      description = "Testing SIZE with int field")
  private int smallSize;

  @Config(key = "ozone.test.config.size.large", type = ConfigType.SIZE,
      defaultValue = "5GB", tags = {},
      description = "Testing SIZE with long field")
  private long largeSize;

  @Config(key = "ozone.test.config.threshold", type = ConfigType.DOUBLE, defaultValue = "10",
      description = "Threshold (To test DOUBLE config type)",
      tags = ConfigTag.MANAGEMENT)
  private double threshold = 10;

  @Config(key = "ozone.test.config.dynamic", reconfigurable = true, defaultValue = "original",
      description = "Test dynamic property", tags = {})
  private String dynamic;

  @Config(key = "ozone.test.config.with.prefix.included", defaultValue = "any",
      description = "Test property whose name includes the group prefix", tags = {})
  private String withPrefix;

  public void setClientAddress(String clientAddress) {
    this.clientAddress = clientAddress;
  }

  public void setBindHost(String bindHost) {
    this.bindHost = bindHost;
  }

  public void setCompressionEnabled(boolean compressionEnabled) {
    this.compressionEnabled = compressionEnabled;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setWaitTime(long waitTime) {
    this.waitTime = waitTime;
  }

  public void setThreshold(double threshold) {
    this.threshold = threshold;
  }

  public void setWithPrefix(String newValue) {
    withPrefix = newValue;
  }

  public String getClientAddress() {
    return clientAddress;
  }

  public String getBindHost() {
    return bindHost;
  }

  public boolean isCompressionEnabled() {
    return compressionEnabled;
  }

  public int getPort() {
    return port;
  }

  public long getWaitTime() {
    return waitTime;
  }

  public Duration getDuration() {
    return duration;
  }

  public double getThreshold() {
    return threshold;
  }

  public String getDynamic() {
    return dynamic;
  }

  public String getWithPrefix() {
    return withPrefix;
  }
}
