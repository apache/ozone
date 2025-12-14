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

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class that manages all bucket operation handlers.
 * Provides a registry pattern for looking up handlers based on query parameters.
 */
public class BucketOperationHandlerFactory {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(BucketOperationHandlerFactory.class);
  
  private final Map<String, BucketOperationHandler> handlers = new HashMap<>();
  
  /**
   * Register all available bucket operation handlers.
   */
  public BucketOperationHandlerFactory() {
    registerDefaultHandlers();
  }
  
  /**
   * Register default handlers for S3 bucket operations.
   */
  private void registerDefaultHandlers() {
    register(new AclHandler());
  }
  
  /**
   * Register a bucket operation handler.
   *
   * @param handler the handler to register
   */
  @VisibleForTesting
  public void register(BucketOperationHandler handler) {
    String queryParam = handler.getQueryParamName();
    if (handlers.containsKey(queryParam)) {
      LOG.warn("Overwriting existing handler for query parameter: {}", 
          queryParam);
    }
    handlers.put(queryParam, handler);
    LOG.debug("Registered handler for query parameter: {}", queryParam);
  }
  
  /**
   * Get a handler for the specified query parameter.
   *
   * @param queryParam the query parameter name
   * @return the corresponding handler, or null if not found
   */
  public BucketOperationHandler getHandler(String queryParam) {
    return handlers.get(queryParam);
  }
  
  /**
   * Check if a handler exists for the specified query parameter.
   *
   * @param queryParam the query parameter name
   * @return true if a handler exists
   */
  public boolean hasHandler(String queryParam) {
    return handlers.containsKey(queryParam);
  }
  
  /**
   * Find the first supported query parameter that has a non-null value.
   *
   * This method iterates through all registered handlers and checks if the
   * corresponding query parameter has a non-null value in the provided map.
   *
   * @param queryParams map of query parameter names to their values
   * @return the name of the first query parameter that has both a non-null value
   *         and a registered handler, or null if none found
   */
  public String findFirstSupportedQueryParam(Map<String, String> queryParams) {
    if (queryParams == null || queryParams.isEmpty()) {
      return null;
    }

    // Iterate through registered handlers and find the first one with a value
    for (Map.Entry<String, BucketOperationHandler> entry : handlers.entrySet()) {
      String paramName = entry.getKey();
      String paramValue = queryParams.get(paramName);

      if (paramValue != null) {
        return paramName;
      }
    }

    return null;
  }

  /**
   * Get all registered query parameter names.
   *
   * @return set of query parameter names
   */
  @VisibleForTesting
  public java.util.Set<String> getRegisteredQueryParams() {
    return handlers.keySet();
  }
}
