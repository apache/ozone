/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client.io;

import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;

/**
 * Provides resources for BlockOutputStream, including byte buffer pool,
 * executor service, and client metrics.
 */
public final class BlockOutPutStreamResourceProvider {
  private final Supplier<ExecutorService> threadFactorySupplier;
  private final ContainerClientMetrics clientMetrics;
  private volatile ExecutorService executorService;

  /**
   * Creates an instance of BlockOutPutStreamResourceProvider.
   */
  public static BlockOutPutStreamResourceProvider create(
      Supplier<ExecutorService> threadFactorySupplier, ContainerClientMetrics clientMetrics) {
    return new BlockOutPutStreamResourceProvider(threadFactorySupplier, clientMetrics);
  }

  private BlockOutPutStreamResourceProvider(Supplier<ExecutorService> threadFactorySupplier,
      ContainerClientMetrics clientMetrics) {
    this.threadFactorySupplier = threadFactorySupplier;
    this.clientMetrics = clientMetrics;
  }

  /**
   * Provides an ExecutorService, lazily initialized upon first request.
   */
  public ExecutorService getThreadFactory() {
    if (executorService == null) {
      synchronized (this) {
        if (executorService == null) {
          executorService = threadFactorySupplier.get();
        }
      }
    }
    return executorService;
  }

  /**
   * Returns the ContainerClientMetrics instance.
   */
  public ContainerClientMetrics getClientMetrics() {
    return clientMetrics;
  }
}
