/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.exceptions;

import java.io.IOException;

/**
 * Exception thrown by Ozone Manager when a transaction is replayed. This
 * exception should not be thrown to client. It is used in
 * OMClientRequest#validateAndUpdateCache to log error and continue in case
 * of replay transaction.
 */
public class OMReplayException extends IOException {

  public OMReplayException() {
    // Dummy message. This exception is not thrown to client.
    super("Replayed transaction");
  }

  public OMReplayException(String message) {
    super(message);
  }
}