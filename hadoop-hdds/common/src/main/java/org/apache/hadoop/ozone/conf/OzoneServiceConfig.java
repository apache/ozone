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

package org.apache.hadoop.ozone.conf;

import static org.apache.hadoop.hdds.conf.ConfigTag.DATANODE;
import static org.apache.hadoop.hdds.conf.ConfigTag.OM;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.RECON;
import static org.apache.hadoop.hdds.conf.ConfigTag.S3GATEWAY;
import static org.apache.hadoop.hdds.conf.ConfigTag.SCM;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;

/**
 * This class is used to define Ozone service level configs which are needed
 * for all the ozone services.
 */
@ConfigGroup(prefix = "ozone.service")
public class OzoneServiceConfig {

  /** Minimum shutdown timeout: {@value} second(s). */
  public static final long OZONE_SHUTDOWN_TIMEOUT_MINIMUM = 1;

  /** The default time unit used: seconds. */
  public static final TimeUnit OZONE_SHUTDOWN_TIME_UNIT_DEFAULT =
          TimeUnit.SECONDS;

  public static final int DEFAULT_SHUTDOWN_HOOK_PRIORITY = 10;

  public static final String SERVICE_SHUTDOWN_TIMEOUT =
      "ozone.service.shutdown.timeout";
  /** Default shutdown hook timeout: {@value} seconds. */
  public static final String SERVICE_SHUTDOWN_TIMEOUT_DEFAULT = "60s";

  @Config(key = SERVICE_SHUTDOWN_TIMEOUT,
      defaultValue = SERVICE_SHUTDOWN_TIMEOUT_DEFAULT,
      type = ConfigType.TIME,
      tags = {OZONE, OM, SCM, DATANODE, RECON, S3GATEWAY},
      timeUnit = TimeUnit.SECONDS,
      description = "Timeout to wait for each shutdown operation to complete" +
          "If a hook takes longer than this time to complete, it will " +
          "be interrupted, so the service will shutdown. This allows the " +
          "service shutdown to recover from a blocked operation. " +
          "The minimum duration of the timeout is 1 second, if " +
          "hook has been configured with a timeout less than 1 second."
  )
  private long serviceShutdownTimeout = 60;

  public long getServiceShutdownTimeout() {
    return serviceShutdownTimeout;
  }

  public void setServiceShutdownTimeout(long serviceShutdownTimeout) {
    this.serviceShutdownTimeout = serviceShutdownTimeout;
  }
}
