/*
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

package org.apache.hadoop.ozone.om.snapshot.exception;

import javax.annotation.Nonnull;

import static org.apache.hadoop.ozone.om.snapshot.exception.SnapshotException.prependSnapshotError;

/**
 * Exception class for processing runtime exceptions in Snapshot module.
 */
public class SnapshotRuntimeException extends RuntimeException {

  private final SnapshotError snapshotError;
  private final String message;

  public SnapshotRuntimeException(String message,
                                  @Nonnull SnapshotError snapshotError) {
    this(message, null, snapshotError);
  }

  public SnapshotRuntimeException(String message, Throwable cause,
                                  @Nonnull SnapshotError snapshotError) {
    super(prependSnapshotError(snapshotError, message), cause);
    this.snapshotError = snapshotError;
    this.message = message;
  }

  public SnapshotRuntimeException(Throwable cause,
                                  @Nonnull SnapshotError snapshotError) {
    this("", cause, snapshotError);
  }

  public SnapshotException convertToCheckedSnapshotException() {
    return new SnapshotException(this.message, this.getCause(),
        this.snapshotError);
  }
}
