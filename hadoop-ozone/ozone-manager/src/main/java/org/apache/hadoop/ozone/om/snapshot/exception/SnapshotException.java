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

import org.apache.logging.log4j.util.Strings;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Exception class for processing exceptions in Snapshot module.
 */
public class SnapshotException extends IOException {
  private final SnapshotError snapshotError;
  private final String message;

  static String prependSnapshotError(SnapshotError snapshotError,
                                      String message) {
    return Strings.isNotBlank(message) ?
        (snapshotError.toString() + ":" + message) : snapshotError.toString();
  }

  public SnapshotException(@Nonnull SnapshotError snapshotError) {
    this("", null, snapshotError);
  }

  public SnapshotException(String message,
                           @Nonnull SnapshotError snapshotError) {
    this(message, null, snapshotError);
  }

  public SnapshotException(String message, Throwable cause,
                           @Nonnull SnapshotError snapshotError) {
    super(prependSnapshotError(snapshotError, message), cause);
    this.snapshotError = snapshotError;
    this.message = message;

  }

  public SnapshotException(Throwable cause,
                           @Nonnull SnapshotError snapshotError) {
    this("", cause, snapshotError);
  }

  public SnapshotRuntimeException convertToSnapshotRuntimeException() {
    return new SnapshotRuntimeException(this.message, this.getCause(),
        this.snapshotError);
  }
}
