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

import java.io.IOException;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadList;
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
    final String uploadIdMarker = queryParams().get(QueryParams.UPLOAD_ID_MARKER);

    if (maxUploads < 1) {
      throw newError(S3ErrorTable.INVALID_ARGUMENT, "max-uploads",
          new Exception("max-uploads must be positive"));
    }

    long startNanos = context.getStartNanos();

    OzoneBucket bucket = context.getVolume().getBucket(bucketName);

    try {
      S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, bucket.getOwner());
      OzoneMultipartUploadList ozoneMultipartUploadList =
          bucket.listMultipartUploads(prefix, keyMarker, uploadIdMarker, maxUploads);

      ListMultipartUploadsResult result = new ListMultipartUploadsResult();
      result.setBucket(bucketName);
      result.setKeyMarker(keyMarker);
      result.setUploadIdMarker(uploadIdMarker);
      result.setNextKeyMarker(ozoneMultipartUploadList.getNextKeyMarker());
      result.setPrefix(prefix);
      result.setNextUploadIdMarker(ozoneMultipartUploadList.getNextUploadIdMarker());
      result.setMaxUploads(maxUploads);
      result.setTruncated(ozoneMultipartUploadList.isTruncated());

      ozoneMultipartUploadList.getUploads().forEach(upload -> result.addUpload(
          new ListMultipartUploadsResult.Upload(
              upload.getKeyName(),
              upload.getUploadId(),
              upload.getCreationTime(),
              S3StorageType.fromReplicationConfig(upload.getReplicationConfig())
          )));
      getMetrics().updateListMultipartUploadsSuccessStats(startNanos);
      return Response.ok(result).build();
    } catch (Exception exception) {
      getMetrics().updateListMultipartUploadsFailureStats(startNanos);
      throw exception;
    }
  }

}
