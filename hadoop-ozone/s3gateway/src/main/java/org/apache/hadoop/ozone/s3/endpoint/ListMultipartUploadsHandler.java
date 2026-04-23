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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Consts.ENCODING_TYPE;

import java.io.IOException;
import java.util.List;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneMultipartUpload;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadList;
import org.apache.hadoop.ozone.s3.commontypes.EncodingTypeObject;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.apache.hadoop.ozone.s3.util.S3StorageType;

/**
 * Handler for listing multipart uploads in a bucket (?uploads query parameter).
 */
class ListMultipartUploadsHandler extends BucketOperationHandler {

  @Override
  Response handleGetRequest(S3RequestContext context, String bucketName)
      throws IOException, OS3Exception {

    if (queryParams().get(QueryParams.UPLOADS) == null) {
      return null; // Not responsible for this request
    }

    context.setAction(S3GAction.LIST_MULTIPART_UPLOAD);

    final String keyMarker = queryParams().get(QueryParams.KEY_MARKER);
    final int maxUploads = Math.min(queryParams().getInt(QueryParams.MAX_UPLOADS, 1000), 1000);
    final String prefix = queryParams().get(QueryParams.PREFIX);
    final String delimiter = queryParams().get(QueryParams.DELIMITER);
    final String encodingType = queryParams().get(QueryParams.ENCODING_TYPE);
    final String uploadIdMarker = queryParams().get(QueryParams.UPLOAD_ID_MARKER);

    if (maxUploads < 1) {
      throw newError(S3ErrorTable.INVALID_ARGUMENT, "max-uploads",
          new Exception("max-uploads must be positive"));
    }
    if (encodingType != null && !encodingType.equals(ENCODING_TYPE)) {
      throw S3ErrorTable.newError(S3ErrorTable.INVALID_ARGUMENT, encodingType);
    }

    long startNanos = context.getStartNanos();

    OzoneBucket bucket = context.getVolume().getBucket(bucketName);

    try {
      S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, bucket.getOwner());
      OzoneMultipartUploadList ozoneMultipartUploadList =
          bucket.listMultipartUploads(prefix, keyMarker, uploadIdMarker, maxUploads);

      ListMultipartUploadsResult result = new ListMultipartUploadsResult();
      result.setBucket(bucketName);
      result.setKeyMarker(EncodingTypeObject.createNullable(keyMarker, encodingType));
      result.setUploadIdMarker(uploadIdMarker);
      result.setPrefix(EncodingTypeObject.createNullable(prefix, encodingType));
      result.setDelimiter(EncodingTypeObject.createNullable(delimiter, encodingType));
      result.setEncodingType(encodingType);
      result.setMaxUploads(maxUploads);

      final String normalizedPrefix = prefix == null ? "" : prefix;
      String prevDir = null;
      String lastProcessedKey = null;
      String lastProcessedUploadId = null;
      int responseItemCount = 0;

      List<OzoneMultipartUpload> pendingUploads = ozoneMultipartUploadList.getUploads();
      int processedUploads = 0;
      for (OzoneMultipartUpload upload : pendingUploads) {
        String keyName = upload.getKeyName();

        if (StringUtils.isNotEmpty(normalizedPrefix) && !keyName.startsWith(normalizedPrefix)) {
          continue;
        }

        String relativeKeyName = keyName.substring(normalizedPrefix.length());
        String currentDirName = null;
        boolean isDirectoryPlaceholder = false;
        if (StringUtils.isNotBlank(delimiter)) {
          int depth = StringUtils.countMatches(relativeKeyName, delimiter);
          if (depth > 0) {
            int delimiterIndex = relativeKeyName.indexOf(delimiter);
            currentDirName = relativeKeyName.substring(0, delimiterIndex);
          } else if (relativeKeyName.endsWith(delimiter)) {
            currentDirName = relativeKeyName.substring(
                0, relativeKeyName.length() - delimiter.length());
            isDirectoryPlaceholder = true;
          }
        }

        if (responseItemCount >= maxUploads) {
          if (StringUtils.isNotBlank(delimiter)
              && currentDirName != null
              && currentDirName.equals(prevDir)) {
            lastProcessedKey = keyName;
            lastProcessedUploadId = upload.getUploadId();
            processedUploads++;
            continue;
          }
          break;
        }

        boolean addedAsPrefix = false;
        if (StringUtils.isNotBlank(delimiter)) {
          if (currentDirName != null && !currentDirName.equals(prevDir)) {
            result.addCommonPrefix(EncodingTypeObject.createNullable(
                normalizedPrefix + currentDirName + delimiter, encodingType));
            prevDir = currentDirName;
            responseItemCount++;
            addedAsPrefix = true;
          } else if (isDirectoryPlaceholder) {
            result.addCommonPrefix(EncodingTypeObject.createNullable(
                normalizedPrefix + relativeKeyName, encodingType));
            responseItemCount++;
            addedAsPrefix = true;
          } else if (currentDirName != null) {
            addedAsPrefix = true;
          }
        }

        if (!addedAsPrefix) {
          result.addUpload(new ListMultipartUploadsResult.Upload(
              EncodingTypeObject.createNullable(upload.getKeyName(), encodingType),
              upload.getUploadId(),
              upload.getCreationTime(),
              S3StorageType.fromReplicationConfig(upload.getReplicationConfig())
          ));
          responseItemCount++;
        }

        lastProcessedKey = keyName;
        lastProcessedUploadId = upload.getUploadId();
        processedUploads++;
      }

      boolean hasMoreUploads =
          processedUploads < pendingUploads.size() || ozoneMultipartUploadList.isTruncated();

      if (responseItemCount >= maxUploads && lastProcessedKey != null && hasMoreUploads) {
        result.setNextKeyMarker(EncodingTypeObject.createNullable(lastProcessedKey, encodingType));
        result.setNextUploadIdMarker(lastProcessedUploadId);
        result.setTruncated(true);
      } else {
        result.setNextKeyMarker(EncodingTypeObject.createNullable(
            ozoneMultipartUploadList.getNextKeyMarker(), encodingType));
        result.setNextUploadIdMarker(ozoneMultipartUploadList.getNextUploadIdMarker());
        result.setTruncated(ozoneMultipartUploadList.isTruncated());
      }

      getMetrics().updateListMultipartUploadsSuccessStats(startNanos);
      return Response.ok(result).build();
    } catch (Exception exception) {
      getMetrics().updateListMultipartUploadsFailureStats(startNanos);
      throw exception;
    }
  }

}
