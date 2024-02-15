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
 * Provides resources for BlockOutputStream, including executor service,
 * and client metrics.
 */
public final class BlockOutputStreamResourceProvider {
  private final Supplier<ExecutorService> executorServiceSupplier;
  private final ContainerClientMetrics clientMetrics;

  /**
   * Creates an instance of blockOutputStreamResourceProvider.
   */
  public static BlockOutputStreamResourceProvider create(
      Supplier<ExecutorService> executorServiceSupplier, ContainerClientMetrics clientMetrics) {
    return new BlockOutputStreamResourceProvider(executorServiceSupplier, clientMetrics);
  }

  private BlockOutputStreamResourceProvider(Supplier<ExecutorService> executorServiceSupplier,
      ContainerClientMetrics clientMetrics) {
    this.executorServiceSupplier = executorServiceSupplier;
    this.clientMetrics = clientMetrics;
  }

  /**
   * Provides an ExecutorService, lazily initialized upon first request.
   */
  public ExecutorService getExecutorService() {
    return executorServiceSupplier.get();
  }

  /**
   * Returns the ContainerClientMetrics instance.
   */
  public ContainerClientMetrics getClientMetrics() {
    return clientMetrics;
  }
}
