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

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command handler to refresh usage info of all volumes.
 */
public class RefreshVolumeUsageCommandHandler implements CommandHandler {

  static final Logger LOG =
      LoggerFactory.getLogger(RefreshVolumeUsageCommandHandler.class);

  private final AtomicInteger invocationCount = new AtomicInteger(0);
  private final MutableRate opsLatencyMs;

  public RefreshVolumeUsageCommandHandler() {
    MetricsRegistry registry = new MetricsRegistry(
        RefreshVolumeUsageCommandHandler.class.getSimpleName());
    this.opsLatencyMs = registry.newRate(Type.refreshVolumeUsageInfo + "Ms");
  }

  @Override
  public void handle(SCMCommand<?> command, OzoneContainer container,
      StateContext context, SCMConnectionManager connectionManager) {
    LOG.info("receive command to refresh usage info of all volumes");
    invocationCount.incrementAndGet();
    final long startTime = Time.monotonicNow();
    container.getVolumeSet().refreshAllVolumeUsage();
    this.opsLatencyMs.add(Time.monotonicNow() - startTime);
  }

  @Override
  public Type getCommandType() {
    return StorageContainerDatanodeProtocolProtos
        .SCMCommandProto.Type.refreshVolumeUsageInfo;
  }

  @Override
  public int getInvocationCount() {
    return invocationCount.get();
  }

  @Override
  public long getAverageRunTime() {
    return (long) this.opsLatencyMs.lastStat().mean();
  }

  @Override
  public long getTotalRunTime() {
    return (long) this.opsLatencyMs.lastStat().total();
  }

  @Override
  public int getQueuedCount() {
    return 0;
  }
}
