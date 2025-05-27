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

package org.apache.hadoop.ozone.container.ozoneimpl;

import java.io.File;

/**
 * This class is used to identify any error that may be seen while scanning a container.
 */
public class ContainerScanError {

  private final File unhealthyFile;
  private final FailureType failureType;
  private final Throwable exception;

  /**
   * Represents the reason a container scan failed and a container should
   * be marked unhealthy.
   */
  public enum FailureType {
    MISSING_CONTAINER_DIR,
    MISSING_METADATA_DIR,
    MISSING_CONTAINER_FILE,
    MISSING_CHUNKS_DIR,
    MISSING_DATA_FILE,
    CORRUPT_CONTAINER_FILE,
    CORRUPT_CHUNK,
    MISSING_CHUNK,
    INCONSISTENT_CHUNK_LENGTH,
    INACCESSIBLE_DB,
    WRITE_FAILURE,
  }

  public ContainerScanError(FailureType failure, File unhealthyFile, Exception exception) {
    this.unhealthyFile = unhealthyFile;
    this.failureType = failure;
    this.exception = exception;
  }

  public File getUnhealthyFile() {
    return unhealthyFile;
  }

  public FailureType getFailureType() {
    return failureType;
  }

  public Throwable getException() {
    return exception;
  }

  @Override
  public String toString() {
    return failureType + " for file " + unhealthyFile + " with exception: " + exception;
  }
}
