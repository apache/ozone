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

package org.apache.hadoop.ozone.jobworker.commands;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.ozone.jobworker.JobworkerConnectionManager;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;

/**
 * Test implementation of AbstractJobworkerCommandHandler.
 */
public class MockCommandHandler extends AbstractJobworkerCommandHandler {
  private final AtomicBoolean processCommandCalled = new AtomicBoolean(false);
  private Exception exceptionToThrow;
  private long processDelayMs = 0;
  private volatile CountDownLatch pauseLatch;
  private JobworkerCommand<?> lastCommand;

  public MockCommandHandler(OMJobworkerCommandProto.Type type, ExecutorService executorService) {
    super(type, executorService);
  }

  @Override
  protected boolean processCommand(JobworkerCommand<?> command,
                                JobworkerStateContext context,
                                JobworkerConnectionManager connectionManager) throws Exception {

    lastCommand = command;
    if (pauseLatch != null) {
      pauseLatch.await();
    }
    if (processDelayMs > 0) {
      Thread.sleep(processDelayMs);
    }
    processCommandCalled.set(true);

    if (exceptionToThrow != null) {
      throw exceptionToThrow;
    }
    return true;
  }

  public boolean wasProcessCommandCalled() {
    return processCommandCalled.get();
  }

  public void setExceptionToThrow(Exception exception) {
    this.exceptionToThrow = exception;
  }

  public void resetProcessCommandCalled() {
    processCommandCalled.set(false);
  }

  public void setProcessDelayMs(long processDelayMs) {
    this.processDelayMs = processDelayMs;
  }

  public void enablePause() {
    this.pauseLatch = new CountDownLatch(1);
  }

  public void releasePause() {
    if (pauseLatch != null) {
      pauseLatch.countDown();
    }
  }

  public JobworkerCommand<?> getLastCommand() {
    return lastCommand;
  }
}
