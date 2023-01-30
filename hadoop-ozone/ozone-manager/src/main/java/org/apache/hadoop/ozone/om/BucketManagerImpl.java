/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Implementation for {@link BucketManager}
 *
 * BucketManager uses MetadataDB to store bucket level information.
 * Keys used in BucketManager for storing data into MetadataDB
 * for BucketInfo:
 * {volume/bucket} -> bucketInfo
 */
public class BucketManagerImpl implements BucketManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(BucketManagerImpl.class);

  private final OMMetadataManager metadataManager;

  public BucketManagerImpl(OMMetadataManager metadataManager) {
    this.metadataManager = metadataManager;
  }

  @Override
  public OmBucketInfo getBucketInfo(String volumeName, String bucketName)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volumeName,
        bucketName);
    try {
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      OmBucketInfo value = metadataManager.getBucketTable().get(bucketKey);
      if (value == null) {
        LOG.debug("bucket: {} not found in volume: {}.", bucketName,
            volumeName);
        // Check parent volume existence
        final String dbVolumeKey = metadataManager.getVolumeKey(volumeName);
        if (metadataManager.getVolumeTable().get(dbVolumeKey) == null) {
          // Parent volume doesn't exist, throw VOLUME_NOT_FOUND
          throw new OMException("Volume not found when getting bucket info",
              VOLUME_NOT_FOUND);
        } else {
          // Parent volume exists, throw BUCKET_NOT_FOUND
          throw new OMException("Bucket not found", BUCKET_NOT_FOUND);
        }
      }

      value = OzoneManagerUtils.resolveLinkBucketLayout(value, metadataManager,
          new HashSet<>());

      return value;
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Exception while getting bucket info for bucket: {}",
            bucketName, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volumeName,
          bucketName);
    }
  }

  @Override
  public List<OmBucketInfo> listBuckets(String volumeName,
      String startBucket, String bucketPrefix, int maxNumOfBuckets)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    return metadataManager.listBuckets(
        volumeName, startBucket, bucketPrefix, maxNumOfBuckets);

  }

  @Override
  public boolean addAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    Objects.requireNonNull(obj);
    Objects.requireNonNull(acl);
    if (!obj.getResourceType().equals(OzoneObj.ResourceType.BUCKET)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "BucketManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    boolean changed = false;
    metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volume, bucket);
    try {
      String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
      OmBucketInfo bucketInfo =
          metadataManager.getBucketTable().get(dbBucketKey);
      if (bucketInfo == null) {
        LOG.debug("Bucket:{}/{} does not exist", volume, bucket);
        throw new OMException("Bucket " + bucket + " is not found",
            BUCKET_NOT_FOUND);
      }

      changed = bucketInfo.addAcl(acl);
      if (changed) {
        metadataManager.getBucketTable().put(dbBucketKey, bucketInfo);
      }
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Add acl operation failed for bucket:{}/{} acl:{}",
            volume, bucket, acl, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volume, bucket);
    }

    return changed;
  }

  @Override
  public boolean removeAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    Objects.requireNonNull(obj);
    Objects.requireNonNull(acl);
    if (!obj.getResourceType().equals(OzoneObj.ResourceType.BUCKET)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "BucketManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    boolean removed = false;
    metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volume, bucket);
    try {
      String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
      OmBucketInfo bucketInfo =
          metadataManager.getBucketTable().get(dbBucketKey);
      if (bucketInfo == null) {
        LOG.debug("Bucket:{}/{} does not exist", volume, bucket);
        throw new OMException("Bucket " + bucket + " is not found",
            BUCKET_NOT_FOUND);
      }
      removed = bucketInfo.removeAcl(acl);
      if (removed) {
        metadataManager.getBucketTable().put(dbBucketKey, bucketInfo);
      }
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Remove acl operation failed for bucket:{}/{} acl:{}",
            volume, bucket, acl, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volume, bucket);
    }
    return removed;
  }

  @Override
  public boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) throws IOException {
    Objects.requireNonNull(obj);
    Objects.requireNonNull(acls);
    if (!obj.getResourceType().equals(OzoneObj.ResourceType.BUCKET)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "BucketManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    metadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volume, bucket);
    try {
      String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
      OmBucketInfo bucketInfo =
          metadataManager.getBucketTable().get(dbBucketKey);
      if (bucketInfo == null) {
        LOG.debug("Bucket:{}/{} does not exist", volume, bucket);
        throw new OMException("Bucket " + bucket + " is not found",
            BUCKET_NOT_FOUND);
      }
      bucketInfo.setAcls(acls);
      metadataManager.getBucketTable().put(dbBucketKey, bucketInfo);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Set acl operation failed for bucket:{}/{} acl:{}",
            volume, bucket, StringUtils.join(",", acls), ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volume, bucket);
    }
    return true;
  }

  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    Objects.requireNonNull(obj);

    if (!obj.getResourceType().equals(OzoneObj.ResourceType.BUCKET)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "BucketManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volume, bucket);
    try {
      String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
      OmBucketInfo bucketInfo =
          metadataManager.getBucketTable().get(dbBucketKey);
      if (bucketInfo == null) {
        LOG.debug("Bucket:{}/{} does not exist", volume, bucket);
        throw new OMException("Bucket " + bucket + " is not found",
            BUCKET_NOT_FOUND);
      }
      return bucketInfo.getAcls();
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Get acl operation failed for bucket:{}/{}.",
            volume, bucket, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volume, bucket);
    }
  }

  @Override
  public boolean checkAccess(OzoneObj ozObject, RequestContext context)
      throws OMException {
    Objects.requireNonNull(ozObject);
    Objects.requireNonNull(context);

    String volume = ozObject.getVolumeName();
    String bucket = ozObject.getBucketName();
    metadataManager.getLock().acquireReadLock(BUCKET_LOCK, volume, bucket);
    try {
      String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
      OmBucketInfo bucketInfo =
          metadataManager.getBucketTable().get(dbBucketKey);
      if (bucketInfo == null) {
        LOG.debug("Bucket:{}/{} does not exist", volume, bucket);
        throw new OMException("Bucket " + bucket + " is not found",
            BUCKET_NOT_FOUND);
      }
      boolean hasAccess = OzoneAclUtil.checkAclRights(bucketInfo.getAcls(),
          context);
      if (LOG.isDebugEnabled()) {
        LOG.debug("user:{} has access rights for bucket:{} :{} ",
            context.getClientUgi(), ozObject.getBucketName(), hasAccess);
      }
      return hasAccess;
    } catch (IOException ex) {
      if (ex instanceof OMException) {
        throw (OMException) ex;
      }
      LOG.error("CheckAccess operation failed for bucket:{}/{}.",
          volume, bucket, ex);
      throw new OMException("Check access operation failed for " +
          "bucket:" + bucket, ex, INTERNAL_ERROR);
    } finally {
      metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volume, bucket);
    }
  }
}
