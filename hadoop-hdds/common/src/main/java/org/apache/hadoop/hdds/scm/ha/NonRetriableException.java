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

package org.apache.hadoop.hdds.scm.ha;

import java.io.IOException;

/**
 * exception for which there should be no retry.
 */
public class NonRetriableException extends IOException {

  /**
   * Constructs a {@code NonRetriableException} with the given detailed message. <p>
   * Required for Unwrapping {@code RemoteException}. Used by
   * {@link org.apache.hadoop.ipc_.RemoteException#unwrapRemoteException()}
   */
  public NonRetriableException(String message) {
    super(message);
  }

  public NonRetriableException(IOException exception) {
    super(exception);
  }
}
