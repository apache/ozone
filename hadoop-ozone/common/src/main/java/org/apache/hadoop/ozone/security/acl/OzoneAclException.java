/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.security.acl;

/**
 * Timeout exception thrown by Ozone. Ex: When checking ACLs for an Object if
 * security manager is not able to process the request in configured time than
 * {@link OzoneAclException} should be thrown.
 */
public class OzoneAclException extends Exception {

  private ErrorCode errorCode;

  /**
   * Constructs a new exception with {@code null} as its detail message. The
   * cause is not initialized, and may subsequently be initialized by a call to
   * {@link #initCause}.
   */
  public OzoneAclException() {
    super("");
  }

  /**
   * Constructs a new exception with {@code null} as its detail message. The
   * cause is not initialized, and may subsequently be initialized by a call to
   * {@link #initCause}.
   */
  public OzoneAclException(String errorMsg, ErrorCode code, Throwable ex) {
    super(errorMsg, ex);
    this.errorCode = code;
  }

  enum ErrorCode {
    TIMEOUT,
    OTHER
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }
}
