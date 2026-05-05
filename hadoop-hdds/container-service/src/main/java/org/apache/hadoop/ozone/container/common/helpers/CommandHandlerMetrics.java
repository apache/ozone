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

package org.apache.hadoop.ozone.container.common.helpers;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.CommandHandler;

/**
 * This class collects and exposes metrics for CommandHandlerMetrics.
 */
@InterfaceAudience.Private
public final class CommandHandlerMetrics implements MetricsSource {
  public static final String SOURCE_NAME = CommandHandlerMetrics.class.getSimpleName();

  private final Map<Type, CommandHandler> handlerMap;
  private final Map<Type, AtomicInteger> commandCount;

  private CommandHandlerMetrics(Map<Type, CommandHandler> handlerMap) {
    this.handlerMap = handlerMap;
    this.commandCount = new HashMap<>();
    handlerMap.forEach((k, v) -> this.commandCount.put(k, new AtomicInteger()));
  }

  /**
   * Creates a new instance of CommandHandlerMetrics and
   * registers it to the DefaultMetricsSystem.
   *
   * @param handlerMap the map of command types to their
   *                  corresponding command handlers
   * @return the registered instance of CommandHandlerMetrics
   */
  public static CommandHandlerMetrics create(
      Map<Type, CommandHandler> handlerMap) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "CommandHandlerMetrics Metrics",
        new CommandHandlerMetrics(handlerMap));
  }

  /**
   * Increases the count of received commands for the specified command type.
   *
   * @param type the type of the command for which the count should be increased
   */
  public void increaseCommandCount(Type type) {
    commandCount.get(type).addAndGet(1);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    for (Map.Entry<Type, CommandHandler> entry : handlerMap.entrySet()) {
      CommandHandler commandHandler = entry.getValue();
      MetricsRecordBuilder builder = collector.addRecord(SOURCE_NAME)
          .setContext("CommandHandlerMetrics")
          .tag(CommandMetricsMetricsInfo.Command,
              commandHandler.getCommandType().name());

      builder.addGauge(CommandMetricsMetricsInfo.TotalRunTimeMs, commandHandler.getTotalRunTime());
      builder.addGauge(CommandMetricsMetricsInfo.AvgRunTimeMs, commandHandler.getAverageRunTime());
      builder.addGauge(CommandMetricsMetricsInfo.QueueWaitingTaskCount, commandHandler.getQueuedCount());
      builder.addGauge(CommandMetricsMetricsInfo.InvocationCount, commandHandler.getInvocationCount());
      int activePoolSize = commandHandler.getThreadPoolActivePoolSize();
      if (activePoolSize >= 0) {
        builder.addGauge(CommandMetricsMetricsInfo.ThreadPoolActivePoolSize, activePoolSize);
      }
      int maxPoolSize = commandHandler.getThreadPoolMaxPoolSize();
      if (maxPoolSize >= 0) {
        builder.addGauge(CommandMetricsMetricsInfo.ThreadPoolMaxPoolSize, maxPoolSize);
      }
      builder.addGauge(CommandMetricsMetricsInfo.CommandReceivedCount,
          commandCount.get(commandHandler.getCommandType()).get());
    }
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  enum CommandMetricsMetricsInfo implements MetricsInfo {
    Command("The type of the SCM command"),
    TotalRunTimeMs("The total runtime of the command handler in milliseconds"),
    AvgRunTimeMs("Average run time of the command handler in milliseconds"),
    QueueWaitingTaskCount("The number of queued tasks waiting for execution"),
    InvocationCount("The number of times the command handler has been invoked"),
    ThreadPoolActivePoolSize("The number of active threads in the thread pool"),
    ThreadPoolMaxPoolSize("The maximum number of threads in the thread pool"),
    CommandReceivedCount(
        "The number of received SCM commands for each command type");

    private final String desc;
    CommandMetricsMetricsInfo(String desc) {
      this.desc = desc;
    }

    @Override
    public String description() {
      return desc;
    }
  }
}
