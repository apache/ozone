/**
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
package org.apache.hadoop.hdds.conf;

import java.util.concurrent.TimeUnit;

/**
 * Example configuration to test the configuration injection.
 */
@ConfigGroup(prefix = "test.scm.client")
public class SimpleConfiguration extends SimpleConfigurationParent {

  @Config(key = "address", defaultValue = "localhost", description = "Client "
      + "address (To test string injection).", tags = ConfigTag.MANAGEMENT)
  private String clientAddress;

  @Config(key = "bind.host", defaultValue = "0.0.0.0", description = "Bind "
      + "host(To test string injection).", tags = ConfigTag.MANAGEMENT)
  private String bindHost;

  @Config(key = "compression.enabled", defaultValue = "true", description =
      "Compression enabled. (Just to test boolean flag)", tags =
      ConfigTag.MANAGEMENT)
  private boolean compressionEnabled;

  @Config(key = "port", defaultValue = "9878", description = "Port number "
      + "config (To test in injection)", tags = ConfigTag.MANAGEMENT)
  private int port;

  @Config(key = "wait", type = ConfigType.TIME, timeUnit =
      TimeUnit.SECONDS, defaultValue = "30m", description = "Wait time (To "
      + "test TIME config type)", tags = ConfigTag.MANAGEMENT)
  private long waitTime;

  @Config(key = "class", type = ConfigType.CLASS,
      defaultValue = "java.lang.Object", description = "",
      tags = ConfigTag.OZONE)
  private Class<?> myClass = Object.class;

  @Config(key = "threshold", type = ConfigType.DOUBLE,
      defaultValue = "10", description = "Threshold (To test DOUBLE config" +
      " type)", tags = ConfigTag.MANAGEMENT)
  private double threshold;

  @PostConstruct
  public void validate() {
    if (port < 0) {
      throw new NumberFormatException("Please use a positive port number");
    }
  }

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

  public Class<?> getMyClass() {
    return myClass;
  }

  public void setMyClass(Class<?> aClass) {
    this.myClass = aClass;
  }

  public double getThreshold() {
    return threshold;
  }

  public void setThreshold(double threshold) {
    this.threshold = threshold;
  }
}
