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

package org.apache.hadoop.ozone.s3.signature;

import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

/**
 * This exception is used to communicate validation errors when parsing
 * signatures.
 */
public class MalformedResourceException extends Exception {
  private final S3ErrorTable errorCode;
  private final String resource;

  public MalformedResourceException(String resource) {
    this(null, null, resource);
  }

  public MalformedResourceException(String message, String resource) {
    this(null, message, resource);
  }

  public MalformedResourceException(S3ErrorTable errorCode, String resource) {
    this(errorCode, null, resource);
  }

  private MalformedResourceException(S3ErrorTable errorCode, String message,
      String resource) {
    super(message);
    this.errorCode = errorCode;
    this.resource = resource;
  }

  public S3ErrorTable getErrorCode() {
    return errorCode;
  }

  public String getResource() {
    return resource;
  }
}
