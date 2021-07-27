/**
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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.update.client;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;

/**
 * Update service configuration.
 */
@ConfigGroup(prefix = "ozone.scm.update.service")
public final class UpdateServiceConfig {

  @Config(key = "port", defaultValue = "9893", description = "Port used for"
      + " the SCM grpc update service for CRL.", tags = {
      ConfigTag.SECURITY})
  private int port;

  public int getPort() {
    return port;
  }

  public UpdateServiceConfig setPort(
      int portParam) {
    this.port = portParam;
    return this;
  }
}
