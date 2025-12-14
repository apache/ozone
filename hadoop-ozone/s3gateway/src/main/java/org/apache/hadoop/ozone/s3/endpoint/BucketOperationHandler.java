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

package org.apache.hadoop.ozone.s3.endpoint;

import java.io.IOException;
import java.io.InputStream;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

/**
 * Interface for handling bucket operations based on query parameters.
 * Each implementation handles a specific S3 bucket subresource operation
 * (e.g., ?acl, ?lifecycle, ?notification).
 */
public interface BucketOperationHandler {

  /**
   * Handle the bucket operation.
   *
   * @param bucketName the name of the bucket
   * @param body the request body stream
   * @param headers the HTTP headers
   * @param context the endpoint context containing shared dependencies
   * @param startNanos the start time in nanoseconds for metrics tracking
   * @return HTTP response
   * @throws IOException if an I/O error occurs
   * @throws OS3Exception if an S3-specific error occurs
   */
  Response handlePutRequest(
      String bucketName,
      InputStream body,
      HttpHeaders headers,
      BucketEndpointContext context,
      long startNanos
  ) throws IOException, OS3Exception;

  /**
   * Get the query parameter name this handler is responsible for.
   * For example: "acl", "lifecycle", "notification"
   *
   * @return the query parameter name
   */
  String getQueryParamName();
}
