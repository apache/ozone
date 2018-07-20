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
package org.apache.hadoop.hdds;

/**
 * This class contains constants for configuration keys and default values
 * used in hdds.
 */
public final class HddsConfigKeys {

  /**
   * Do not instantiate.
   */
  private HddsConfigKeys() {
  }

  public static final String HDDS_HEARTBEAT_INTERVAL =
      "hdds.heartbeat.interval";
  public static final String HDDS_HEARTBEAT_INTERVAL_DEFAULT =
      "30s";

  public static final String HDDS_NODE_REPORT_INTERVAL =
      "hdds.node.report.interval";
  public static final String HDDS_NODE_REPORT_INTERVAL_DEFAULT =
      "60s";

  public static final String HDDS_CONTAINER_REPORT_INTERVAL =
      "hdds.container.report.interval";
  public static final String HDDS_CONTAINER_REPORT_INTERVAL_DEFAULT =
      "60s";

  public static final String HDDS_COMMAND_STATUS_REPORT_INTERVAL =
      "hdds.command.status.report.interval";
  public static final String HDDS_COMMAND_STATUS_REPORT_INTERVAL_DEFAULT =
      "60s";
}
