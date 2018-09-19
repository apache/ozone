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
package org.apache.hadoop.hdds.scm.exceptions;

import java.io.IOException;

/**
 * Exception thrown by SCM.
 */
public class SCMException extends IOException {
  private final ResultCodes result;

  /**
   * Constructs an {@code IOException} with {@code null}
   * as its error detail message.
   */
  public SCMException(ResultCodes result) {
    this.result = result;
  }

  /**
   * Constructs an {@code IOException} with the specified detail message.
   *
   * @param message The detail message (which is saved for later retrieval by
   * the
   * {@link #getMessage()} method)
   */
  public SCMException(String message, ResultCodes result) {
    super(message);
    this.result = result;
  }

  /**
   * Constructs an {@code IOException} with the specified detail message
   * and cause.
   * <p>
   * <p> Note that the detail message associated with {@code cause} is
   * <i>not</i> automatically incorporated into this exception's detail
   * message.
   *
   * @param message The detail message (which is saved for later retrieval by
   * the
   * {@link #getMessage()} method)
   * @param cause The cause (which is saved for later retrieval by the {@link
   * #getCause()} method).  (A null value is permitted, and indicates that the
   * cause is nonexistent or unknown.)
   * @since 1.6
   */
  public SCMException(String message, Throwable cause, ResultCodes result) {
    super(message, cause);
    this.result = result;
  }

  /**
   * Constructs an {@code IOException} with the specified cause and a
   * detail message of {@code (cause==null ? null : cause.toString())}
   * (which typically contains the class and detail message of {@code cause}).
   * This constructor is useful for IO exceptions that are little more
   * than wrappers for other throwables.
   *
   * @param cause The cause (which is saved for later retrieval by the {@link
   * #getCause()} method).  (A null value is permitted, and indicates that the
   * cause is nonexistent or unknown.)
   * @since 1.6
   */
  public SCMException(Throwable cause, ResultCodes result) {
    super(cause);
    this.result = result;
  }

  /**
   * Returns resultCode.
   * @return ResultCode
   */
  public ResultCodes getResult() {
    return result;
  }

  /**
   * Error codes to make it easy to decode these exceptions.
   */
  public enum ResultCodes {
    SUCCEESS,
    FAILED_TO_LOAD_NODEPOOL,
    FAILED_TO_FIND_NODE_IN_POOL,
    FAILED_TO_FIND_HEALTHY_NODES,
    FAILED_TO_FIND_NODES_WITH_SPACE,
    FAILED_TO_FIND_SUITABLE_NODE,
    INVALID_CAPACITY,
    INVALID_BLOCK_SIZE,
    CHILL_MODE_EXCEPTION,
    FAILED_TO_LOAD_OPEN_CONTAINER,
    FAILED_TO_ALLOCATE_CONTAINER,
    FAILED_TO_CHANGE_CONTAINER_STATE,
    FAILED_TO_CHANGE_PIPELINE_STATE,
    CONTAINER_EXISTS,
    FAILED_TO_FIND_CONTAINER,
    FAILED_TO_FIND_CONTAINER_WITH_SPACE,
    BLOCK_EXISTS,
    FAILED_TO_FIND_BLOCK,
    IO_EXCEPTION,
    UNEXPECTED_CONTAINER_STATE,
    SCM_NOT_INITIALIZED,
    DUPLICATE_DATANODE,
    NO_SUCH_DATANODE,
    NO_REPLICA_FOUND,
    FAILED_TO_FIND_ACTIVE_PIPELINE
  }
}
