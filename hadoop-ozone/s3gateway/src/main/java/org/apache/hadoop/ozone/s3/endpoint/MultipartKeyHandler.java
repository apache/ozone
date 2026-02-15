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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_UPLOAD;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;

import java.io.IOException;
import java.time.Instant;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3StorageType;

/**
 * Handles MPU (Multipart Upload) non-POST operations for object key endpoint.
 */
public class MultipartKeyHandler extends ObjectOperationHandler {

  @Override
  Response handleGetRequest(ObjectEndpoint.ObjectRequestContext context, String keyPath)
      throws IOException, OS3Exception {

    final String uploadId = queryParams().get(QueryParams.UPLOAD_ID);
    if (uploadId == null) {
      // not MPU -> let next handler run
      return null;
    }

    context.setAction(S3GAction.LIST_PARTS);

    final int maxParts = queryParams().getInt(QueryParams.MAX_PARTS, 1000);
    final String partNumberMarker = queryParams().get(QueryParams.PART_NUMBER_MARKER);
    final long startNanos = context.getStartNanos();
    final AuditLogger.PerformanceStringBuilder perf = context.getPerf();

    try {
      int partMarker = parsePartNumberMarker(partNumberMarker);
      Response response = listParts(context.getBucket(), keyPath, uploadId,
          partMarker, maxParts, perf);
      long opLatencyNs = getMetrics().updateListPartsSuccessStats(startNanos);
      perf.appendOpLatencyNanos(opLatencyNs);
      return response;

    } catch (IOException | RuntimeException ex) {
      getMetrics().updateListPartsFailureStats(startNanos);
      throw ex;
    }
  }

  @Override
  Response handleDeleteRequest(ObjectEndpoint.ObjectRequestContext context, String keyPath)
      throws IOException, OS3Exception {

    final String uploadId = queryParams().get(QueryParams.UPLOAD_ID);
    if (StringUtils.isEmpty(uploadId)) {
      // not MPU -> let next handler run
      return null;
    }

    context.setAction(S3GAction.ABORT_MULTIPART_UPLOAD);

    final long startNanos = context.getStartNanos();
    try {
      Response r = abortMultipartUpload(context.getVolume(),
          context.getBucketName(), keyPath, uploadId);

      getMetrics().updateAbortMultipartUploadSuccessStats(startNanos);
      return r;

    } catch (IOException | RuntimeException ex) {
      getMetrics().updateAbortMultipartUploadFailureStats(startNanos);
      throw ex;
    }
  }

  /**
   * Abort multipart upload request.
   * @param bucket
   * @param key
   * @param uploadId
   * @return Response
   * @throws IOException
   * @throws OS3Exception
   */
  private Response abortMultipartUpload(OzoneVolume volume, String bucket,
      String key, String uploadId) throws IOException, OS3Exception {
    try {
      getClientProtocol().abortMultipartUpload(volume.getName(), bucket, key, uploadId);
    } catch (OMException ex) {
      if (ex.getResult() == ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR) {
        throw newError(S3ErrorTable.NO_SUCH_UPLOAD, uploadId, ex);
      } else if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucket, ex);
      }
      throw ex;
    }
    return Response.status(Status.NO_CONTENT).build();
  }

  /**
   * Returns response for the listParts request.
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadListParts.html
   * @param ozoneBucket
   * @param key
   * @param uploadId
   * @param partNumberMarker
   * @param maxParts
   * @return
   * @throws IOException
   * @throws OS3Exception
   */
  private Response listParts(OzoneBucket ozoneBucket, String key, String uploadId,
      int partNumberMarker, int maxParts,
      org.apache.hadoop.ozone.audit.AuditLogger.PerformanceStringBuilder perf)
      throws IOException, OS3Exception {

    ListPartsResponse resp = new ListPartsResponse();
    String bucketName = ozoneBucket.getName();

    try {
      OzoneMultipartUploadPartListParts parts =
          ozoneBucket.listParts(key, uploadId, partNumberMarker, maxParts);

      resp.setBucket(bucketName);
      resp.setKey(key);
      resp.setUploadID(uploadId);
      resp.setMaxParts(maxParts);
      resp.setPartNumberMarker(partNumberMarker);
      resp.setTruncated(false);

      resp.setStorageClass(S3StorageType.fromReplicationConfig(
          parts.getReplicationConfig()).toString());

      if (parts.isTruncated()) {
        resp.setTruncated(true);
        resp.setNextPartNumberMarker(parts.getNextPartNumberMarker());
      }

      parts.getPartInfoList().forEach(p -> {
        ListPartsResponse.Part part = new ListPartsResponse.Part();
        part.setPartNumber(p.getPartNumber());
        part.setETag(StringUtils.isNotEmpty(p.getETag()) ? p.getETag() : p.getPartName());
        part.setSize(p.getSize());
        part.setLastModified(Instant.ofEpochMilli(p.getModificationTime()));
        resp.addPart(part);
      });

    } catch (OMException ex) {
      if (ex.getResult() == ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR) {
        throw newError(NO_SUCH_UPLOAD, uploadId, ex);
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED,
            bucketName + "/" + key + "/" + uploadId, ex);
      }
      throw ex;
    }

    perf.appendCount(resp.getPartList().size());
    return Response.status(Status.OK).entity(resp).build();
  }
}
