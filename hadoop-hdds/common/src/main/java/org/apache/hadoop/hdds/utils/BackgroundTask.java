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

package org.apache.hadoop.hdds.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.RecursiveTask;

/**
 * A task thread to run by {@link BackgroundService}.
 */
public abstract class BackgroundTask extends RecursiveTask<BackgroundTask.BackgroundTaskForkResult>
    implements Callable<BackgroundTaskResult> {
  private static final long serialVersionUID = 1L;

  static final class BackgroundTaskForkResult {
    private final BackgroundTaskResult result;
    private final Throwable throwable;
    private final long startTime;
    private final long endTime;

    private BackgroundTaskForkResult(BackgroundTaskResult result, long startTime, long endTime, Throwable throwable) {
      this.endTime = endTime;
      this.result = result;
      this.startTime = startTime;
      this.throwable = throwable;
    }

    public long getTotalExecutionTime() {
      return endTime - startTime;
    }

    public BackgroundTaskResult getResult() {
      return result;
    }

    public Throwable getThrowable() {
      return throwable;
    }
  }

  @Override
  protected final BackgroundTaskForkResult compute() {
    long startTime = System.nanoTime();
    BackgroundTaskResult result = null;
    Throwable throwable = null;
    try {
      result = this.call();
    } catch (Throwable e) {
      throwable = e;
    }
    long endTime = System.nanoTime();
    return new BackgroundTaskForkResult(result, startTime, endTime, throwable);
  }

  public int getPriority() {
    return 0;
  }
}
