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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.USER_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.VOLUME_LOCK;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Volume Manager implementation.
 */
public class VolumeManagerImpl implements VolumeManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(VolumeManagerImpl.class);

  private final OMMetadataManager metadataManager;

  public VolumeManagerImpl(OMMetadataManager metadataManager) {
    this.metadataManager = metadataManager;
  }

  @Override
  public OmVolumeArgs getVolumeInfo(String volume) throws IOException {
    Objects.requireNonNull(volume, "volume == null");
    metadataManager.getLock().acquireReadLock(VOLUME_LOCK, volume);
    try {
      String dbVolumeKey = metadataManager.getVolumeKey(volume);
      OmVolumeArgs volumeArgs =
          metadataManager.getVolumeTable().get(dbVolumeKey);
      if (volumeArgs == null) {
        LOG.debug("volume:{} does not exist", volume);
        throw new OMException("Volume " + volume + " is not found",
            ResultCodes.VOLUME_NOT_FOUND);
      }

      return volumeArgs;
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.warn("Info volume failed for volume:{}", volume, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseReadLock(VOLUME_LOCK, volume);
    }
  }

  @Override
  public List<OmVolumeArgs> listVolumes(String userName,
      String prefix, String startKey, int maxKeys) throws IOException {
    boolean acquired = false;
    if (userName != null) {
      acquired = metadataManager.getLock().acquireReadLock(USER_LOCK, userName)
          .isLockAcquired();
    }
    try {
      return metadataManager.listVolumes(userName, prefix, startKey, maxKeys);
    } finally {
      if (acquired) {
        metadataManager.getLock().releaseReadLock(USER_LOCK, userName);
      }
    }
  }

  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    Objects.requireNonNull(obj);

    if (!obj.getResourceType().equals(OzoneObj.ResourceType.VOLUME)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "VolumeManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    metadataManager.getLock().acquireReadLock(VOLUME_LOCK, volume);
    try {
      String dbVolumeKey = metadataManager.getVolumeKey(volume);
      OmVolumeArgs volumeArgs =
          metadataManager.getVolumeTable().get(dbVolumeKey);
      if (volumeArgs == null) {
        LOG.debug("volume:{} does not exist", volume);
        throw new OMException("Volume " + volume + " is not found",
            ResultCodes.VOLUME_NOT_FOUND);
      }

      Preconditions.checkState(volume.equals(volumeArgs.getVolume()));
      return volumeArgs.getAcls();
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Get acl operation failed for volume:{}", volume, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releaseReadLock(VOLUME_LOCK, volume);
    }
  }

  @Override
  public boolean checkAccess(OzoneObj ozObject, RequestContext context)
      throws OMException {
    Objects.requireNonNull(ozObject);
    Objects.requireNonNull(context);

    String volume = ozObject.getVolumeName();
    metadataManager.getLock().acquireReadLock(VOLUME_LOCK, volume);
    try {
      String dbVolumeKey = metadataManager.getVolumeKey(volume);
      OmVolumeArgs volumeArgs =
          metadataManager.getVolumeTable().get(dbVolumeKey);
      if (volumeArgs == null) {
        LOG.debug("volume:{} does not exist", volume);
        throw new OMException("Volume " + volume + " is not found",
            ResultCodes.VOLUME_NOT_FOUND);
      }

      Preconditions.checkState(volume.equals(volumeArgs.getVolume()));
      boolean hasAccess = OzoneAclUtil.checkAclRights(
          volumeArgs.getAcls(), context);
      if (LOG.isDebugEnabled()) {
        LOG.debug("user:{} has access rights for volume:{} :{} ",
            context.getClientUgi(), ozObject.getVolumeName(), hasAccess);
      }
      return hasAccess;
    } catch (IOException ex) {
      if (ex instanceof OMException) {
        throw (OMException) ex;
      }
      LOG.error("Check access operation failed for volume:{}", volume, ex);
      throw new OMException("Check access operation failed for " +
          "volume:" + volume, ex, ResultCodes.INTERNAL_ERROR);
    } finally {
      metadataManager.getLock().releaseReadLock(VOLUME_LOCK, volume);
    }
  }
}
