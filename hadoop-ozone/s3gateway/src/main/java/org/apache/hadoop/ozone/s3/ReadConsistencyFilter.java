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

package org.apache.hadoop.ozone.s3;

import static org.apache.hadoop.ozone.s3.util.S3Consts.LOCAL_LEASE_LOG_LIMIT_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.READ_CONSISTENCY_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Utils.wrapOS3Exception;

import java.io.IOException;
import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.ReadConsistencyContext;

/**
 * Validates S3 read consistency headers before resource construction.
 */
@Provider
@PreMatching
@Priority(ReadConsistencyFilter.PRIORITY)
public class ReadConsistencyFilter implements ContainerRequestFilter {
  public static final int PRIORITY = HeaderPreprocessor.PRIORITY +
      S3GatewayHttpServer.FILTER_PRIORITY_DO_AFTER;

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    try {
      ReadConsistencyContext.fromHeaders(
          requestContext.getHeaderString(READ_CONSISTENCY_HEADER),
          requestContext.getHeaderString(LOCAL_LEASE_LOG_LIMIT_HEADER));
    } catch (OS3Exception ex) {
      throw wrapOS3Exception(ex);
    }
  }
}
