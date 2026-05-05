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

package org.apache.hadoop.hdds.scm.pipeline;

import java.io.IOException;

/**
 * Exception thrown when there are not enough Datanodes to create a pipeline.
 */
public final class InsufficientDatanodesException extends IOException {

  private final int required;
  private final int available;

  /**
   * Required for Unwrapping {@code RemoteException}. Used by
   * {@link org.apache.hadoop.ipc_.RemoteException#unwrapRemoteException()}
   */
  public InsufficientDatanodesException(String message) {
    super(message);
    this.required = 0;
    this.available = 0;
  }

  public InsufficientDatanodesException(int required, int available,
      String message) {
    super(message);
    this.required = required;
    this.available = available;
  }

  public InsufficientDatanodesException(int required, int available) {
    this(required, available, "Not enough datanodes" +
        ", requested: " + required +
        ", found: " + available
    );
  }

  public int getRequiredNodes() {
    return required;
  }

  public int getAvailableNodes() {
    return available;
  }
}
