/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.hdds.scm.update.client;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;

import java.time.Duration;

import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.SCM;
import static org.apache.hadoop.hdds.conf.ConfigTag.SECURITY;

/**
 * Configuration used by SCM CRL update client.
 */
@ConfigGroup(prefix = "ozone.scm.update")
public class SCMUpdateClientConfiguration {
  @Config(key = "client.crl.check.interval",
      type = ConfigType.TIME,
      defaultValue = "600s",
      tags = {SCM, OZONE, SECURITY},
      description = "The interval that the scm update service client use to" +
          "check its pending CRLs."
  )
  private long clientCrlCheckIntervalInMs =
      Duration.ofMinutes(10).toMillis();

  public long getClientCrlCheckInterval() {
    return clientCrlCheckIntervalInMs;
  }

  public void setClientCrlCheckInterval(Duration interval) {
    this.clientCrlCheckIntervalInMs = interval.toMillis();
  }
}
