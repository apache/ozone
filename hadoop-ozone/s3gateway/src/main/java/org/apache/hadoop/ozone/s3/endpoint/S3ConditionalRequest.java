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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_REQUEST;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.PRECOND_FAILED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Utils.parseETag;

import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.OptionalLong;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;

/**
 * Shared parsing and evaluation for S3 conditional request headers.
 */
final class S3ConditionalRequest {

  private S3ConditionalRequest() {
  }

  static Response evaluateReadPreconditions(HttpHeaders headers,
      String keyPath, OzoneKey key) throws OS3Exception {
    String currentETag = key.getMetadata().get(OzoneConsts.ETAG);
    String ifMatch = headers.getHeaderString(S3Consts.IF_MATCH_HEADER);
    if (ifMatch != null && !eTagMatches(ifMatch, currentETag)) {
      throw newError(PRECOND_FAILED, keyPath);
    }

    String ifUnmodifiedSince = headers.getHeaderString(
        S3Consts.IF_UNMODIFIED_SINCE_HEADER);
    if (ifMatch == null && ifUnmodifiedSince != null
        && !matchesIfUnmodifiedSince(key, ifUnmodifiedSince)) {
      throw newError(PRECOND_FAILED, keyPath);
    }

    String ifNoneMatch = headers.getHeaderString(
        S3Consts.IF_NONE_MATCH_HEADER);
    if (ifNoneMatch != null) {
      if (eTagMatches(ifNoneMatch, currentETag)) {
        return buildNotModifiedResponse(key);
      }
      return null;
    }

    String ifModifiedSince = headers.getHeaderString(
        S3Consts.IF_MODIFIED_SINCE_HEADER);
    if (ifModifiedSince != null && !matchesIfModifiedSince(key,
        ifModifiedSince)) {
      return buildNotModifiedResponse(key);
    }

    return null;
  }

  static boolean checkCopySourceModificationTime(Long lastModificationTime,
      String copySourceIfModifiedSinceStr,
      String copySourceIfUnmodifiedSinceStr) {
    long copySourceIfModifiedSince = Long.MIN_VALUE;
    long copySourceIfUnmodifiedSince = Long.MAX_VALUE;

    OptionalLong modifiedDate =
        parseAndValidatePastOrPresentDate(copySourceIfModifiedSinceStr);
    if (modifiedDate.isPresent()) {
      copySourceIfModifiedSince = modifiedDate.getAsLong();
    }

    OptionalLong unmodifiedDate =
        parseAndValidatePastOrPresentDate(copySourceIfUnmodifiedSinceStr);
    if (unmodifiedDate.isPresent()) {
      copySourceIfUnmodifiedSince = unmodifiedDate.getAsLong();
    }
    return (copySourceIfModifiedSince <= lastModificationTime)
        && (lastModificationTime <= copySourceIfUnmodifiedSince);
  }

  static WriteConditions parseWriteConditions(HttpHeaders headers,
      String keyPath) throws OS3Exception {
    String ifNoneMatch = headers.getHeaderString(S3Consts.IF_NONE_MATCH_HEADER);
    if (ifNoneMatch != null && StringUtils.isBlank(ifNoneMatch)) {
      OS3Exception ex = newError(INVALID_REQUEST, keyPath);
      ex.setErrorMessage("If-None-Match header cannot be empty.");
      throw ex;
    }

    String ifMatch = headers.getHeaderString(S3Consts.IF_MATCH_HEADER);
    if (ifMatch != null && StringUtils.isBlank(ifMatch)) {
      OS3Exception ex = newError(INVALID_REQUEST, keyPath);
      ex.setErrorMessage("If-Match header cannot be empty.");
      throw ex;
    }

    String trimmedIfNoneMatch = ifNoneMatch == null ? null : ifNoneMatch.trim();
    String trimmedIfMatch = ifMatch == null ? null : ifMatch.trim();

    if (trimmedIfNoneMatch != null && trimmedIfMatch != null) {
      OS3Exception ex = newError(INVALID_REQUEST, keyPath);
      ex.setErrorMessage(
          "If-Match and If-None-Match cannot be specified together.");
      throw ex;
    }

    if (trimmedIfNoneMatch != null
        && !"*".equals(parseETag(trimmedIfNoneMatch))) {
      OS3Exception ex = newError(INVALID_REQUEST, keyPath);
      ex.setErrorMessage(
          "Only If-None-Match: * is supported for conditional put.");
      throw ex;
    }

    return new WriteConditions(trimmedIfNoneMatch, trimmedIfMatch);
  }

  private static Response buildNotModifiedResponse(OzoneKey key) {
    ResponseBuilder responseBuilder = Response.status(Status.NOT_MODIFIED);
    ObjectEndpoint.addEntityTagHeader(responseBuilder, key);
    ObjectEndpoint.addLastModifiedDate(responseBuilder, key);
    return responseBuilder.build();
  }

  private static boolean eTagMatches(String headerValue, String currentETag) {
    if (headerValue == null) {
      return false;
    }
    for (String candidate : headerValue.split(",")) {
      String parsedCandidate = parseETag(candidate);
      if ("*".equals(parsedCandidate)) {
        return true;
      }
      if (currentETag != null
          && currentETag.equals(parsedCandidate)) {
        return true;
      }
    }
    return false;
  }

  private static boolean matchesIfModifiedSince(OzoneKey key,
      String headerValue) {
    Instant since = parseConditionalInstant(headerValue);
    if (since == null) {
      return true;
    }
    Instant lastModified = key.getModificationTime()
        .truncatedTo(ChronoUnit.SECONDS);
    return lastModified.isAfter(since);
  }

  private static boolean matchesIfUnmodifiedSince(OzoneKey key,
      String headerValue) {
    Instant since = parseConditionalInstant(headerValue);
    if (since == null) {
      return true;
    }
    Instant lastModified = key.getModificationTime()
        .truncatedTo(ChronoUnit.SECONDS);
    return !lastModified.isAfter(since);
  }

  private static Instant parseConditionalInstant(String headerValue) {
    if (headerValue == null) {
      return null;
    }
    try {
      return Instant.ofEpochMilli(OzoneUtils.formatDate(headerValue))
          .truncatedTo(ChronoUnit.SECONDS);
    } catch (IllegalArgumentException | ParseException ex) {
      return null;
    }
  }

  private static OptionalLong parseAndValidatePastOrPresentDate(
      String ozoneDateStr) {
    if (ozoneDateStr == null) {
      return OptionalLong.empty();
    }

    long ozoneDateInMs;
    try {
      ozoneDateInMs = OzoneUtils.formatDate(ozoneDateStr);
    } catch (ParseException e) {
      return OptionalLong.empty();
    }

    long currentDate = System.currentTimeMillis();
    if (ozoneDateInMs <= currentDate) {
      return OptionalLong.of(ozoneDateInMs);
    }
    return OptionalLong.empty();
  }

  static final class WriteConditions {
    private final String ifNoneMatch;
    private final String ifMatch;

    private WriteConditions(String ifNoneMatch, String ifMatch) {
      this.ifNoneMatch = ifNoneMatch;
      this.ifMatch = ifMatch;
    }

    boolean hasIfNoneMatch() {
      return ifNoneMatch != null;
    }

    boolean hasIfMatch() {
      return ifMatch != null;
    }

    String getExpectedETag() {
      return parseETag(ifMatch);
    }
  }
}
