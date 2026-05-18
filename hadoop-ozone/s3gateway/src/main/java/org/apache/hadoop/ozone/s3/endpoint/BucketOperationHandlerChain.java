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
import java.util.LinkedList;
import java.util.List;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

/** Chain of responsibility for {@link BucketOperationHandler}s. */
final class BucketOperationHandlerChain extends BucketOperationHandler {

  private final List<BucketOperationHandler> handlers;

  private BucketOperationHandlerChain(List<BucketOperationHandler> handlers) {
    this.handlers = handlers;
  }

  @Override
  Response handleDeleteRequest(S3RequestContext context, String bucketName) throws IOException, OS3Exception {
    for (BucketOperationHandler handler : handlers) {
      Response response = handler.handleDeleteRequest(context, bucketName);
      if (response != null) {
        return response;
      }
    }
    return null;
  }

  @Override
  Response handleGetRequest(S3RequestContext context, String bucketName) throws IOException, OS3Exception {
    for (BucketOperationHandler handler : handlers) {
      Response response = handler.handleGetRequest(context, bucketName);
      if (response != null) {
        return response;
      }
    }
    return null;
  }

  @Override
  Response handlePutRequest(S3RequestContext context, String bucketName, InputStream body)
      throws IOException, OS3Exception {
    for (BucketOperationHandler handler : handlers) {
      Response response = handler.handlePutRequest(context, bucketName, body);
      if (response != null) {
        return response;
      }
    }
    return null;
  }

  static Builder newBuilder(BucketEndpoint endpoint) {
    return new Builder(endpoint);
  }

  /** Builds {@code BucketOperationHandlerChain}. */
  static final class Builder {
    private final List<BucketOperationHandler> handlers = new LinkedList<>();
    private final BucketEndpoint endpoint;

    private Builder(BucketEndpoint endpoint) {
      this.endpoint = endpoint;
    }

    /** Append {@code handler} to the list of delegates. */
    Builder add(BucketOperationHandler handler) {
      handlers.add(handler.copyDependenciesFrom(endpoint));
      return this;
    }

    /** Create {@code BucketOperationHandlerChain} with the list of delegates. */
    BucketOperationHandler build() {
      return new BucketOperationHandlerChain(handlers)
          .copyDependenciesFrom(endpoint);
    }
  }
}
