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
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

/**
 * Context object that provides access to BucketEndpoint resources.
 * This allows handlers to access endpoint functionality without
 * tight coupling to the BucketEndpoint class.
 * 
 * Since BucketEndpoint extends EndpointBase, handlers can access:
 * - Bucket and Volume operations
 * - Methods inherited from EndpointBase
 */
public class BucketEndpointContext {
  
  private final BucketEndpoint endpoint;
  
  public BucketEndpointContext(BucketEndpoint endpoint) {
    this.endpoint = endpoint;
  }
  
  /**
   * Get the bucket object.
   * Delegates to BucketEndpoint's inherited getBucket() from EndpointBase.
   *
   * @param bucketName the bucket name
   * @return OzoneBucket instance
   * @throws IOException if bucket cannot be retrieved
   * @throws OS3Exception if S3-specific error occurs
   */
  public OzoneBucket getBucket(String bucketName) 
      throws IOException, OS3Exception {
    return endpoint.getBucket(bucketName);
  }
  
  /**
   * Get the volume object.
   * Delegates to BucketEndpoint's inherited getVolume() from EndpointBase.
   *
   * @return OzoneVolume instance
   * @throws IOException if volume cannot be retrieved
   * @throws OS3Exception if S3-specific error occurs
   */
  public OzoneVolume getVolume() throws IOException, OS3Exception {
    return endpoint.getVolume();
  }
  
  /**
   * Check if an exception indicates access denied.
   * This checks for OMException.ResultCodes that indicate permission issues.
   *
   * @param ex the exception to check
   * @return true if access is denied
   */
  public boolean isAccessDenied(Exception ex) {
    // Check if it's an OMException with ACCESS_DENIED result code
    if (ex instanceof OMException) {
      OMException omEx = (OMException) ex;
      return omEx.getResult() == OMException.ResultCodes.PERMISSION_DENIED ||
             omEx.getResult() == OMException.ResultCodes.ACCESS_DENIED;
    }
    return false;
  }
  
  /**
   * Audit a write operation failure.
   * Delegates to BucketEndpoint's inherited auditWriteFailure() from EndpointBase.
   *
   * @param action the audit action being performed
   * @param ex the exception that occurred
   */
  public void auditWriteFailure(AuditAction action, Throwable ex) {
    endpoint.auditWriteFailure(action, ex);
  }

  /**
   * Get reference to the endpoint for accessing other methods.
   * Use with caution - prefer adding specific methods to this context
   * rather than exposing the entire endpoint.
   *
   * @return BucketEndpoint instance
   */
  protected BucketEndpoint getEndpoint() {
    return endpoint;
  }
}
