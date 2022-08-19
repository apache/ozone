/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3.endpoint;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.Auditor;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;
import org.apache.hadoop.ozone.s3.util.AuditUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;

/**
 * Basic helpers for all the REST endpoints.
 */
public abstract class EndpointBase implements Auditor {

  @Inject
  private OzoneClient client;
  @Inject
  private S3Auth s3Auth;
  @Context
  private ContainerRequestContext context;

  private static final Logger LOG =
      LoggerFactory.getLogger(EndpointBase.class);

  protected static final AuditLogger AUDIT =
      new AuditLogger(AuditLoggerType.S3GLOGGER);

  protected OzoneBucket getBucket(OzoneVolume volume, String bucketName)
      throws OS3Exception, IOException {
    OzoneBucket bucket;
    try {
      bucket = volume.getBucket(bucketName);
    } catch (OMException ex) {
      if (ex.getResult() == ResultCodes.KEY_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName, ex);
      } else if (ex.getResult() == ResultCodes.INVALID_TOKEN) {
        throw newError(S3ErrorTable.ACCESS_DENIED,
            s3Auth.getAccessID(), ex);
      } else if (ex.getResult() == ResultCodes.TIMEOUT ||
          ex.getResult() == ResultCodes.INTERNAL_ERROR) {
        throw newError(S3ErrorTable.INTERNAL_ERROR, bucketName, ex);
      } else {
        throw ex;
      }
    }
    return bucket;
  }

  /**
   * Initializes the object post construction. Calls init() from any
   * child classes to work around the issue of only one method can be annotated.
   */
  @PostConstruct
  public void initialization() {
    LOG.debug("S3 access id: {}", s3Auth.getAccessID());
    getClient().getObjectStore()
        .getClientProxy()
        .setThreadLocalS3Auth(s3Auth);
    init();
  }

  public abstract void init();

  protected OzoneBucket getBucket(String bucketName)
      throws OS3Exception, IOException {
    OzoneBucket bucket;
    try {
      bucket = client.getObjectStore().getS3Bucket(bucketName);
    } catch (OMException ex) {
      if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND
          || ex.getResult() == ResultCodes.VOLUME_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName, ex);
      } else if (ex.getResult() == ResultCodes.INVALID_TOKEN) {
        throw newError(S3ErrorTable.ACCESS_DENIED,
            s3Auth.getAccessID(), ex);
      } else if (ex.getResult() == ResultCodes.PERMISSION_DENIED) {
        throw newError(S3ErrorTable.ACCESS_DENIED, bucketName, ex);
      } else if (ex.getResult() == ResultCodes.TIMEOUT ||
          ex.getResult() == ResultCodes.INTERNAL_ERROR) {
        throw newError(S3ErrorTable.INTERNAL_ERROR, bucketName, ex);
      } else {
        throw ex;
      }
    }
    return bucket;
  }

  protected OzoneVolume getVolume() throws IOException {
    return client.getObjectStore().getS3Volume();
  }

  /**
   * Create an S3Bucket, and also it creates mapping needed to access via
   * ozone and S3.
   * @param bucketName
   * @return location of the S3Bucket.
   * @throws IOException
   */
  protected String createS3Bucket(String bucketName) throws
      IOException, OS3Exception {
    try {
      client.getObjectStore().createS3Bucket(bucketName);
    } catch (OMException ex) {
      getMetrics().incCreateBucketFailure();
      if (ex.getResult() == ResultCodes.PERMISSION_DENIED) {
        throw newError(S3ErrorTable.ACCESS_DENIED, bucketName, ex);
      } else if (ex.getResult() == ResultCodes.INVALID_TOKEN) {
        throw newError(S3ErrorTable.ACCESS_DENIED,
            s3Auth.getAccessID(), ex);
      } else if (ex.getResult() == ResultCodes.TIMEOUT ||
          ex.getResult() == ResultCodes.INTERNAL_ERROR) {
        throw newError(S3ErrorTable.INTERNAL_ERROR, bucketName, ex);
      } else if (ex.getResult() != ResultCodes.BUCKET_ALREADY_EXISTS) {
        // S3 does not return error for bucket already exists, it just
        // returns the location.
        throw ex;
      }
    }
    return "/" + bucketName;
  }

  /**
   * Deletes an s3 bucket and removes mapping of Ozone volume/bucket.
   * @param s3BucketName - S3 Bucket Name.
   * @throws  IOException in case the bucket cannot be deleted.
   */
  protected void deleteS3Bucket(String s3BucketName)
      throws IOException, OS3Exception {
    try {
      client.getObjectStore().deleteS3Bucket(s3BucketName);
    } catch (OMException ex) {
      if (ex.getResult() == ResultCodes.PERMISSION_DENIED) {
        throw newError(S3ErrorTable.ACCESS_DENIED,
            s3BucketName, ex);
      } else if (ex.getResult() == ResultCodes.INVALID_TOKEN) {
        throw newError(S3ErrorTable.ACCESS_DENIED,
            s3Auth.getAccessID(), ex);
      } else if (ex.getResult() == ResultCodes.TIMEOUT ||
          ex.getResult() == ResultCodes.INTERNAL_ERROR) {
        throw newError(S3ErrorTable.INTERNAL_ERROR, s3BucketName, ex);
      } else {
        throw ex;
      }
    }
  }

  /**
   * Returns Iterator to iterate over all buckets for a specific user.
   * The result can be restricted using bucket prefix, will return all
   * buckets if bucket prefix is null.
   *
   * @param prefix Bucket prefix to match
   * @return {@code Iterator<OzoneBucket>}
   */
  protected Iterator<? extends OzoneBucket> listS3Buckets(String prefix)
      throws IOException, OS3Exception {
    return iterateBuckets(volume -> volume.listBuckets(prefix));
  }

  /**
   * Returns Iterator to iterate over all buckets after prevBucket for a
   * specific user. If prevBucket is null it returns an iterator to iterate
   * over all buckets for this user. The result can be restricted using
   * bucket prefix, will return all buckets if bucket prefix is null.
   *
   * @param prefix Bucket prefix to match
   * @param previousBucket Buckets are listed after this bucket
   * @return {@code Iterator<OzoneBucket>}
   */
  protected Iterator<? extends OzoneBucket> listS3Buckets(String prefix,
      String previousBucket) throws IOException, OS3Exception {
    return iterateBuckets(volume -> volume.listBuckets(prefix, previousBucket));
  }

  private Iterator<? extends OzoneBucket> iterateBuckets(
      Function<OzoneVolume, Iterator<? extends OzoneBucket>> query)
      throws IOException, OS3Exception {
    try {
      return query.apply(getVolume());
    } catch (OMException e) {
      if (e.getResult() == ResultCodes.VOLUME_NOT_FOUND) {
        return Collections.emptyIterator();
      } else  if (e.getResult() == ResultCodes.PERMISSION_DENIED) {
        throw newError(S3ErrorTable.ACCESS_DENIED,
            "listBuckets", e);
      } else if (e.getResult() == ResultCodes.INVALID_TOKEN) {
        throw newError(S3ErrorTable.ACCESS_DENIED,
            s3Auth.getAccessID(), e);
      } else if (e.getResult() == ResultCodes.TIMEOUT ||
          e.getResult() == ResultCodes.INTERNAL_ERROR) {
        throw newError(S3ErrorTable.INTERNAL_ERROR,
            "listBuckets", e);
      } else {
        throw e;
      }
    }
  }

  private AuditMessage.Builder auditMessageBaseBuilder(AuditAction op,
      Map<String, String> auditMap) {
    AuditMessage.Builder builder = new AuditMessage.Builder()
        .forOperation(op)
        .withParams(auditMap);
    if (s3Auth != null &&
        s3Auth.getAccessID() != null &&
        !s3Auth.getAccessID().isEmpty()) {
      builder.setUser(s3Auth.getAccessID());
    }
    if (context != null) {
      builder.atIp(AuditUtils.getClientIpAddress(context));
    }
    return builder;
  }

  @Override
  public AuditMessage buildAuditMessageForSuccess(AuditAction op,
      Map<String, String> auditMap) {
    AuditMessage.Builder builder = auditMessageBaseBuilder(op, auditMap)
        .withResult(AuditEventStatus.SUCCESS);
    return builder.build();
  }

  @Override
  public AuditMessage buildAuditMessageForFailure(AuditAction op,
      Map<String, String> auditMap, Throwable throwable) {
    AuditMessage.Builder builder = auditMessageBaseBuilder(op, auditMap)
        .withResult(AuditEventStatus.FAILURE)
        .withException(throwable);
    return builder.build();
  }


  @VisibleForTesting
  public void setClient(OzoneClient ozoneClient) {
    this.client = ozoneClient;
  }

  public OzoneClient getClient() {
    return client;
  }

  protected ClientProtocol getClientProtocol() {
    return getClient().getProxy();
  }

  @VisibleForTesting
  public S3GatewayMetrics getMetrics() {
    return S3GatewayMetrics.create();
  }

  protected Map<String, String> getAuditParameters() {
    return AuditUtils.getAuditParameters(context);
  }

  protected void auditWriteFailure(AuditAction action, Throwable ex) {
    AUDIT.logWriteFailure(
        buildAuditMessageForFailure(action, getAuditParameters(), ex));
  }

  protected void auditReadFailure(AuditAction action, Exception ex) {
    AUDIT.logReadFailure(
        buildAuditMessageForFailure(action, getAuditParameters(), ex));
  }

  protected boolean isAccessDenied(OMException ex) {
    ResultCodes result = ex.getResult();
    return result == ResultCodes.PERMISSION_DENIED
        || result == ResultCodes.INVALID_TOKEN;
  }

}
