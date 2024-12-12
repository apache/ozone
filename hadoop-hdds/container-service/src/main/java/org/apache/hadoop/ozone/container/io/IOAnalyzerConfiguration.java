/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.container.io;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

@ConfigGroup(prefix = "hdds.datanode.io")
public class IOAnalyzerConfiguration {

  public static final long IO_ANALYZER_INTERVAL_DEFAULT =
      Duration.ofDays(7).toMillis();

  @Config(key = "enabled",
      type = ConfigType.BOOLEAN,
      defaultValue = "true",
      tags = {ConfigTag.DATANODE},
      description = "Config parameter to enable datanode io analyzer.")
  private boolean enabled = true;

  @Config(key = "io.analyzer.interval",
      type = ConfigType.TIME,
      defaultValue = "30s",
      tags = {ConfigTag.STORAGE},
      description = "The time interval for acquiring IO data is set to 30s.")
  private long iOAnalyzerInterval = IO_ANALYZER_INTERVAL_DEFAULT;

  public long getIOAnalyzerInterval() {
    return iOAnalyzerInterval;
  }
}
