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

package org.apache.hadoop.ozone.jobworker;

import java.time.Duration;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;

/**
 * Configuration specific to JobWorker.
 */
@ConfigGroup(prefix = "ozone.jobworker")
public class JobworkerConfiguration {

  @Config(key = "ozone.jobworker.rpc.timeout",
      type = ConfigType.TIME,
      tags = {ConfigTag.JOBWORKER},
      defaultValue = "10s",
      description = "Timeout for RPC."
  )
  private Duration rpcTimeout;

  public Duration getRpcTimeout() {
    return rpcTimeout;
  }

  public void setRpcTimeout(Duration rpcTimeout) {
    this.rpcTimeout = rpcTimeout;
  }

  @Config(key = "ozone.jobworker.grpc.maximum.inbound.length",
      defaultValue = "32M",
      type = ConfigType.SIZE,
      tags = {ConfigTag.JOBWORKER},
      description = "Maximum inbound message size for gRPC.")
  private int grpcMaximumInboundLength;

  public int getGrpcMaximumInboundLength() {
    return grpcMaximumInboundLength;
  }

  public void setGrpcMaximumInboundLength(int grpcMaximumInboundLength) {
    this.grpcMaximumInboundLength = grpcMaximumInboundLength;
  }

  @Config(key = "ozone.jobworker.heartbeat.interval",
      type = ConfigType.TIME,
      tags = {ConfigTag.JOBWORKER},
      defaultValue = "30s",
      description = "Heartbeat interval from JobWorker to OM."
  )
  private Duration heartbeatInterval = Duration.ofSeconds(30);

  public Duration getHeartbeatInterval() {
    return heartbeatInterval;
  }

  public void setHeartbeatInterval(Duration heartbeatInterval) {
    this.heartbeatInterval = heartbeatInterval;
  }

  @Config(key = "ozone.jobworker.heartbeat.log.warn.interval",
      type = ConfigType.INT,
      tags = {ConfigTag.JOBWORKER},
      defaultValue = "10",
      description = "Number of heartbeat failures before logging a warning."
  )
  private int heartbeatLogWarnInterval = 10;

  public int getHeartbeatLogWarnInterval() {
    return heartbeatLogWarnInterval;
  }

  public void setHeartbeatLogWarnInterval(int heartbeatLogWarnInterval) {
    this.heartbeatLogWarnInterval = heartbeatLogWarnInterval;
  }

  @Config(key = "ozone.jobworker.storage.volume.dirs",
      type = ConfigType.STRING,
      tags = {ConfigTag.JOBWORKER},
      defaultValue = "",
      description = "Comma separated directories where JobWorker will store data."
  )
  private String storageDirs = "";

  public String getStorageDirs() {
    return storageDirs;
  }

  public void setStorageVolumeDirs(String dirs) {
    this.storageDirs = dirs;
  }

  @Config(key = "ozone.jobworker.node.report.interval",
      type = ConfigType.TIME,
      tags = {ConfigTag.JOBWORKER},
      defaultValue = "60s",
      description = "Interval for sending node reports from JobWorker to OM.")
  private Duration nodeReportInterval = Duration.ofSeconds(60);

  public Duration getNodeReportInterval() {
    return nodeReportInterval;
  }

  public void setNodeReportInterval(Duration nodeReportInterval) {
    this.nodeReportInterval = nodeReportInterval;
  }

  @Config(key = "ozone.jobworker.max.report.count",
      type = ConfigType.INT,
      tags = {ConfigTag.JOBWORKER},
      defaultValue = "4096",
      description = "The maximum number of reports that a Jobworker can send to OM at one time.")
  private int maxReportCount = 4096;

  public int getMaxReportCount() {
    return maxReportCount;
  }

  public void setMaxReportCount(int maxReportCount) {
    this.maxReportCount = maxReportCount;
  }

  @Config(key = "ozone.jobworker.max.report.size",
      type = ConfigType.SIZE,
      tags = {ConfigTag.JOBWORKER},
      defaultValue = "8M",
      description = "The maximum size of reports that a Jobworker can send to OM at one time.")
  private int maxReportSizeInBytes = 8 * 1024 * 1024;

  public int getMaxReportSizeInBytes() {
    return maxReportSizeInBytes;
  }

  public void setMaxReportSizeInBytes(int maxReportSizeInBytes) {
    this.maxReportSizeInBytes = maxReportSizeInBytes;
  }

  @Config(key = "ozone.jobworker.command.queue.limit",
      type = ConfigType.INT,
      tags = {ConfigTag.JOBWORKER},
      defaultValue = "100",
      description = "Maximum number of commands that can be queued."
  )
  private int commandQueueLimit = 100;

  public int getCommandQueueLimit() {
    return commandQueueLimit;
  }

  public void setCommandQueueLimit(int commandQueueLimit) {
    this.commandQueueLimit = commandQueueLimit;
  }

  @Config(key = "ozone.jobworker.command.status.report.interval",
      type = ConfigType.TIME,
      tags = {ConfigTag.JOBWORKER},
      defaultValue = "30s",
      description = "Interval for sending command status reports from JobWorker to OM."
  )
  private Duration commandStatusReportInterval = Duration.ofSeconds(30);

  public Duration getCommandStatusReportInterval() {
    return commandStatusReportInterval;
  }

  public void setCommandStatusReportInterval(Duration commandStatusReportInterval) {
    this.commandStatusReportInterval = commandStatusReportInterval;
  }

  @Config(key = "ozone.jobworker.om.service.ids",
      type = ConfigType.STRING,
      tags = {ConfigTag.JOBWORKER, ConfigTag.OM},
      defaultValue = "",
      description = "Comma-separated list of OM service Ids for jobworker to connect. Note that these OM service IDs" +
          " should have the same cluster ID (i.e. connects to the same SCM service) since jobworker currently " +
          " can only serve a cluster. This is the main reason why there is a separate configuration from the usual" +
          " ozone.om.service.ids. The ozone.om.service.ids will be used by the jobworker for other use cases " +
          " such as creating OzoneClient that will be used for bucket replication.")
  private String omServiceIds;

  public static final String OZONE_JOBWORKER_OM_SERVICE_IDS_KEY = "ozone.jobworker.om.service.ids";

  public String getOmServiceIds() {
    return omServiceIds;
  }

  public void setOmServiceIds(String omServiceIds) {
    this.omServiceIds = omServiceIds;
  }

}
