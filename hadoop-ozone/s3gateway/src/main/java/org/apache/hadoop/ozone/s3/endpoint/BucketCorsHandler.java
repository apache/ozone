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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ARGUMENT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_XML;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_CORS_CONFIGURATION;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.helpers.CorsConfiguration;
import org.apache.hadoop.ozone.om.helpers.CorsRule;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.apache.http.HttpStatus;
import org.apache.ratis.util.MemoizedSupplier;

/**
 * Handler for bucket CORS operations (?cors query parameter).
 */
public class BucketCorsHandler extends BucketOperationHandler {
  private static final int MAX_RULES = 100;
  private static final int MAX_RULE_ID_LENGTH = 255;
  private static final Set<String> ALLOWED_METHODS = new HashSet<>(
      Arrays.asList("GET", "PUT", "POST", "DELETE", "HEAD"));
  private static final MemoizedSupplier<MessageUnmarshaller<S3BucketCors>>
      UNMARSHALLER =
      MemoizedSupplier.valueOf(() -> new MessageUnmarshaller<>(
          S3BucketCors.class));

  private boolean shouldHandle() {
    return queryParams().contains(QueryParams.CORS);
  }

  @Override
  Response handleGetRequest(S3RequestContext context, String bucketName)
      throws IOException, OS3Exception {
    if (!shouldHandle()) {
      return null;
    }
    context.setAction(S3GAction.GET_BUCKET_CORS);

    OzoneBucket bucket = context.getBucket(bucketName);
    S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName,
        bucket.getOwner());
    CorsConfiguration corsConfiguration = bucket.getCorsConfiguration();
    if (corsConfiguration == null) {
      throw newError(NO_SUCH_CORS_CONFIGURATION, bucketName);
    }
    return Response.ok(
        S3BucketCors.fromCorsConfiguration(corsConfiguration),
        MediaType.APPLICATION_XML_TYPE).build();
  }

  @Override
  Response handlePutRequest(S3RequestContext context, String bucketName,
      InputStream body) throws IOException, OS3Exception {
    if (!shouldHandle()) {
      return null;
    }
    context.setAction(S3GAction.PUT_BUCKET_CORS);

    OzoneBucket bucket = context.getBucket(bucketName);
    S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName,
        bucket.getOwner());
    S3BucketCors cors;
    try {
      cors = UNMARSHALLER.get().readFrom(body);
    } catch (WebApplicationException ex) {
      throw newError(MALFORMED_XML, bucketName, ex);
    }
    CorsConfiguration corsConfiguration =
        cors.toCorsConfiguration();
    validate(corsConfiguration, bucketName);
    bucket.setCorsConfiguration(corsConfiguration);
    return Response.status(HttpStatus.SC_OK).build();
  }

  @Override
  Response handleDeleteRequest(S3RequestContext context, String bucketName)
      throws IOException, OS3Exception {
    if (!shouldHandle()) {
      return null;
    }
    context.setAction(S3GAction.DELETE_BUCKET_CORS);

    OzoneBucket bucket = context.getBucket(bucketName);
    S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName,
        bucket.getOwner());
    bucket.deleteCorsConfiguration();
    return Response.status(HttpStatus.SC_NO_CONTENT).build();
  }

  private static void validate(CorsConfiguration corsConfiguration,
      String bucketName) throws OS3Exception {
    if (corsConfiguration.getRules().isEmpty()
        || corsConfiguration.getRules().size() > MAX_RULES) {
      throw newError(INVALID_ARGUMENT, bucketName);
    }
    for (CorsRule rule : corsConfiguration.getRules()) {
      if (StringUtils.isNotEmpty(rule.getId())
          && rule.getId().length() > MAX_RULE_ID_LENGTH) {
        throw newError(INVALID_ARGUMENT, bucketName);
      }
      if (rule.getAllowedOrigins().isEmpty()
          || rule.getAllowedMethods().isEmpty()) {
        throw newError(INVALID_ARGUMENT, bucketName);
      }
      for (String method : rule.getAllowedMethods()) {
        if (!ALLOWED_METHODS.contains(method.toUpperCase(Locale.ROOT))) {
          throw newError(INVALID_ARGUMENT, bucketName);
        }
      }
      validateWildcardCount(rule.getAllowedOrigins(), bucketName);
      validateWildcardCount(rule.getAllowedHeaders(), bucketName);
    }
  }

  private static void validateWildcardCount(Iterable<String> values,
      String bucketName) throws OS3Exception {
    for (String value : values) {
      if (StringUtils.countMatches(value, '*') > 1) {
        throw newError(INVALID_ARGUMENT, bucketName);
      }
    }
  }
}
