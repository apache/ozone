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

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Consts.ENCODING_TYPE;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.s3.commontypes.EncodingTypeObject;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.ContinueToken;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the S3 {@code GET Bucket (List Objects)} operation: reads the listing
 * parameters from the request's query parameters, validates them, lists the
 * bucket's keys and builds the {@link ListObjectResponse}.
 */
final class BucketListing {

  private static final Logger LOG = LoggerFactory.getLogger(BucketListing.class);

  private final S3RequestContext context;
  private final BucketEndpoint endpoint;
  private final String bucketName;
  private final String delimiter;
  private final String encodingType;
  private final String marker;
  private final String continueToken;
  private String prefix;
  private String startAfter;
  private int maxKeys;
  private OzoneBucket bucket;
  private Iterator<? extends OzoneKey> ozoneKeyIterator;
  private ContinueToken decodedToken;

  private BucketListing(S3RequestContext context, BucketEndpoint endpoint, String bucketName) {
    this.context = context;
    this.endpoint = endpoint;
    this.bucketName = bucketName;
    this.delimiter = endpoint.queryParams().get(QueryParams.DELIMITER);
    this.encodingType = endpoint.queryParams().get(QueryParams.ENCODING_TYPE);
    this.marker = endpoint.queryParams().get(QueryParams.MARKER);
    this.maxKeys = endpoint.queryParams().getInt(QueryParams.MAX_KEYS, 1000);
    this.prefix = endpoint.queryParams().get(QueryParams.PREFIX);
    this.continueToken = endpoint.queryParams().get(QueryParams.CONTINUATION_TOKEN);
    this.startAfter = endpoint.queryParams().get(QueryParams.START_AFTER);
  }

  static BucketListing fromQueryParams(S3RequestContext context, BucketEndpoint endpoint,
      String bucketName) throws IOException, OS3Exception {
    BucketListing listing = new BucketListing(context, endpoint, bucketName);
    listing.validateAndPrepare();
    return listing;
  }

  Response buildResponse() {
    ListObjectResponse response = initializeListObjectResponse();
    processKeyListing(response);
    return buildFinalResponse(response);
  }

  int getMaxKeys() {
    return maxKeys;
  }

  String getPrefix() {
    return prefix;
  }

  private void validateAndPrepare() throws OS3Exception, IOException {
    validateEncodingType();
    maxKeys = validateMaxKeys(maxKeys);

    if (prefix == null) {
      prefix = "";
    }

    if (startAfter == null && marker != null) {
      startAfter = marker;
    }

    decodedToken = ContinueToken.decodeFromString(continueToken);
    String prevKey = continueToken != null ? decodedToken.getLastKey() : startAfter;
    boolean shallow = endpoint.isListKeysShallowEnabled()
        && OZONE_URI_DELIMITER.equals(delimiter);

    bucket = context.getVolume().getBucket(bucketName);
    S3Owner.verifyBucketOwnerCondition(endpoint.getHeaders(), bucketName, bucket.getOwner());
    try {
      ozoneKeyIterator = bucket.listKeys(prefix, prevKey, shallow);
    } catch (OMException ex) {
      if (ex.getResult() == ResultCodes.FILE_NOT_FOUND) {
        LOG.debug("Key not found for prefix: {}", prefix);
        ozoneKeyIterator = null;
      } else {
        throw ex;
      }
    }
  }

  private void validateEncodingType() throws OS3Exception {
    if (encodingType != null && !encodingType.equals(ENCODING_TYPE)) {
      throw S3ErrorTable.newError(S3ErrorTable.INVALID_ARGUMENT, encodingType);
    }
  }

  private int validateMaxKeys(int value) throws OS3Exception {
    if (value < 0) {
      throw newError(S3ErrorTable.INVALID_ARGUMENT, "maxKeys must be >= 0");
    }

    return Math.min(value, endpoint.getMaxKeysLimit());
  }

  private ListObjectResponse initializeListObjectResponse() {
    ListObjectResponse response = new ListObjectResponse();
    response.setDelimiter(EncodingTypeObject.createNullable(delimiter, encodingType));
    response.setName(bucketName);
    response.setPrefix(EncodingTypeObject.createNullable(prefix, encodingType));
    response.setMarker(marker == null ? "" : marker);
    response.setMaxKeys(maxKeys);
    response.setEncodingType(encodingType);
    response.setTruncated(false);
    response.setContinueToken(continueToken);
    response.setStartAfter(EncodingTypeObject.createNullable(startAfter, encodingType));
    return response;
  }

  private void processKeyListing(ListObjectResponse response) {
    String prevDir = continueToken != null ? decodedToken.getLastDir() : null;
    String lastKey = null;
    int count = 0;

    if (maxKeys > 0) {
      while (ozoneKeyIterator != null && ozoneKeyIterator.hasNext()) {
        OzoneKey next = ozoneKeyIterator.next();

        if (StringUtils.isNotEmpty(prefix) && !next.getName().startsWith(prefix)) {
          continue;
        }

        if (startAfter != null && count == 0 && Objects.equals(startAfter, next.getName())) {
          continue;
        }

        String relativeKeyName = next.getName().substring(prefix.length());
        int depth = StringUtils.countMatches(relativeKeyName, delimiter);

        if (!StringUtils.isEmpty(delimiter)) {
          if (depth > 0) {
            String dirName = relativeKeyName.substring(0, relativeKeyName.indexOf(delimiter));
            if (!dirName.equals(prevDir)) {
              response.addPrefix(EncodingTypeObject.createNullable(
                  prefix + dirName + delimiter, encodingType));
              prevDir = dirName;
              count++;
            }
          } else if (relativeKeyName.endsWith(delimiter)) {
            response.addPrefix(EncodingTypeObject.createNullable(relativeKeyName, encodingType));
            count++;
          } else {
            endpoint.addKeyMetadata(response, next);
            count++;
          }
        } else {
          endpoint.addKeyMetadata(response, next);
          count++;
        }

        if (count == maxKeys) {
          lastKey = next.getName();
          break;
        }
      }
    }

    response.setKeyCount(count);

    if (count < maxKeys) {
      response.setTruncated(false);
    } else if (ozoneKeyIterator != null && ozoneKeyIterator.hasNext() && lastKey != null) {
      response.setTruncated(true);
      ContinueToken nextToken = new ContinueToken(lastKey, prevDir);
      response.setNextToken(nextToken.encodeToString());
      response.setNextMarker(lastKey);
    } else {
      response.setTruncated(false);
    }
  }

  private Response buildFinalResponse(ListObjectResponse response) {
    int keyCount = response.getCommonPrefixes().size() + response.getContents().size();
    long opLatencyNs = endpoint.getMetrics().updateGetBucketSuccessStats(context.getStartNanos());
    endpoint.getMetrics().incListKeyCount(keyCount);
    context.getPerf().appendCount(keyCount);
    context.getPerf().appendOpLatencyNanos(opLatencyNs);
    response.setKeyCount(keyCount);
    return Response.ok(response).build();
  }
}
