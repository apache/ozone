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
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

/**
 * Interface for handling bucket operations using chain of responsibility pattern.
 * Each implementation handles a specific S3 bucket subresource operation
 * (e.g., ?acl, ?lifecycle, ?notification).
 *
 * Implementations should extend EndpointBase to inherit all required functionality
 * (configuration, headers, request context, audit logging, metrics, etc.).
 */
abstract class BucketOperationHandler extends EndpointBase {

  /**
   * Handle the bucket PUT operation if this handler is responsible for it.
   * The handler inspects the request (query parameters, headers, etc.) to determine
   * if it should handle the request.
   *
   * @param bucketName the name of the bucket
   * @param body the request body stream
   * @return Response if this handler handles the request, null otherwise
   * @throws IOException if an I/O error occurs
   * @throws OS3Exception if an S3-specific error occurs
   */
  Response handlePutRequest(S3RequestContext context, String bucketName, InputStream body)
      throws IOException, OS3Exception {
    return null;
  }

  /**
   * Handle the bucket GET operation if this handler is responsible for it.
   * The handler inspects the request (query parameters, headers, etc.) to determine
   * if it should handle the request.
   *
   * @param context
   * @param bucketName the name of the bucket
   * @return Response if this handler handles the request, null otherwise
   * @throws IOException if an I/O error occurs
   * @throws OS3Exception if an S3-specific error occurs
   */
  Response handleGetRequest(S3RequestContext context, String bucketName)
      throws IOException, OS3Exception {
    return null;
  }

  Response handleDeleteRequest(S3RequestContext context, String bucketName)
      throws IOException, OS3Exception {
    return null;
  }
}
