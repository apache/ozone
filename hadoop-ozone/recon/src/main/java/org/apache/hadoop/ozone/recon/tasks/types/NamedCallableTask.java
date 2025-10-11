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

package org.apache.hadoop.ozone.recon.tasks.types;

import java.util.concurrent.Callable;

/**
 * This class is a wrapper over the {@link java.util.concurrent.Callable} interface.
 * <br>
 * If there is any runtime exception which occurs during execution of a task via the executor,
 * we lose all data regarding the task, so we do not have information on which task actually failed.
 * <br>
 * This class is useful, as it will associate a background task to a task name so that we can efficiently
 * record any failure.
 */
public class NamedCallableTask<V> implements Callable<V> {
  private final String taskName;
  private final Callable<V> task;

  public NamedCallableTask(String taskName, Callable<V> task) {
    this.taskName = taskName;
    this.task = task;
  }

  public String getTaskName() {
    return this.taskName;
  }

  @Override
  public V call() throws Exception {
    return task.call();
  }
}
