/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;

import java.util.concurrent.Callable;

/**
 * Abstract class for OmBucketReadWriteFileOps/KeyOps Freon class
 * implementations.
 */
public abstract class AbstractOmBucketReadWriteOps extends BaseFreonGenerator
    implements Callable<Void> {

  /**
   * Generates a synthetic read file/key operations workload.
   *
   * @return total files/keys read
   * @throws Exception
   */
  public abstract int readOperations() throws Exception;

  /**
   * Generates a synthetic write file/key operations workload.
   *
   * @return total files/keys written
   * @throws Exception
   */
  public abstract int writeOperations() throws Exception;

  /**
   * Creates a file/key under the given directory/path.
   *
   * @param path
   * @throws Exception
   */
  public abstract void create(String path) throws Exception;
}
