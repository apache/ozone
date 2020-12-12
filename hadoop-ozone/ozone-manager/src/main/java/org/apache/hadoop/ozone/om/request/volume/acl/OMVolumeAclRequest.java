/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.volume.acl;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.scm.storage.CheckedBiFunction;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

/**
 * Base class for OMVolumeAcl Request.
 */
public abstract class OMVolumeAclRequest extends OMVolumeRequest {

  private CheckedBiFunction<List<OzoneAcl>, OmVolumeArgs, IOException>
      omVolumeAclOp;

  public OMVolumeAclRequest(OzoneManagerProtocolProtos.OMRequest omRequest,
      CheckedBiFunction<List<OzoneAcl>, OmVolumeArgs, IOException> aclOp) {
    super(omRequest);
    omVolumeAclOp = aclOp;
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    // protobuf guarantees volume and acls are non-null.
    String volume = getVolumeName();
    List<OzoneAcl> ozoneAcls = getAcls();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumVolumeUpdates();
    OmVolumeArgs omVolumeArgs = null;

    OMResponse.Builder omResponse = onInit();
    OMClientResponse omClientResponse = null;
    IOException exception = null;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean lockAcquired = false;
    Result result = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
            volume, null, null);
      }
      lockAcquired = omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, volume);
      omVolumeArgs = getVolumeInfo(omMetadataManager, volume);

      // result is false upon add existing acl or remove non-existing acl
      boolean applyAcl = true;
      try {
        omVolumeAclOp.apply(ozoneAcls, omVolumeArgs);
      } catch (OMException ex) {
        applyAcl = false;
      }

      // Update only when
      if (applyAcl) {
        // Update the modification time when updating ACLs of Volume.
        long modificationTime = omVolumeArgs.getModificationTime();
        if (getOmRequest().getAddAclRequest().hasObj()) {
          modificationTime = getOmRequest().getAddAclRequest()
              .getModificationTime();
        } else if (getOmRequest().getSetAclRequest().hasObj()){
          modificationTime = getOmRequest().getSetAclRequest()
              .getModificationTime();
        } else if (getOmRequest().getRemoveAclRequest().hasObj()) {
          modificationTime = getOmRequest().getRemoveAclRequest()
              .getModificationTime();
        }
        omVolumeArgs.setModificationTime(modificationTime);

        omVolumeArgs.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

        // update cache.
        omMetadataManager.getVolumeTable().addCacheEntry(
            new CacheKey<>(omMetadataManager.getVolumeKey(volume)),
            new CacheValue<>(Optional.of(omVolumeArgs), trxnLogIndex));
      }

      omClientResponse = onSuccess(omResponse, omVolumeArgs, applyAcl);
      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omMetrics.incNumVolumeUpdateFails();
      omClientResponse = onFailure(omResponse, ex);
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
      if (lockAcquired) {
        omMetadataManager.getLock().releaseWriteLock(VOLUME_LOCK, volume);
      }
    }

    onComplete(result, exception, trxnLogIndex);

    return omClientResponse;
  }

  /**
   * Get the Acls from the request.
   * @return List of OzoneAcls, for add/remove it is a single element list
   * for set it can be non-single element list.
   */
  abstract List<OzoneAcl> getAcls();

  /**
   * Get the volume name from the request.
   * @return volume name
   * This is needed for case where volume does not exist and the omVolumeArgs is
   * null.
   */
  abstract String getVolumeName();

  // TODO: Finer grain metrics can be moved to these callbacks. They can also
  // be abstracted into separate interfaces in future.
  /**
   * Get the initial om response builder with lock.
   * @return om response builder.
   */
  abstract OMResponse.Builder onInit();

  /**
   * Get the om client response on success case with lock.
   * @param omResponse
   * @param omVolumeArgs
   * @param aclApplied
   * @return OMClientResponse
   */
  abstract OMClientResponse onSuccess(
      OMResponse.Builder omResponse, OmVolumeArgs omVolumeArgs,
      boolean aclApplied);

  /**
   * Get the om client response on failure case with lock.
   * @param omResponse
   * @param ex
   * @return OMClientResponse
   */
  abstract OMClientResponse onFailure(OMResponse.Builder omResponse,
      IOException ex);

  /**
   * Completion hook for final processing before return without lock.
   * Usually used for logging without lock.
   * @param ex
   */
  abstract void onComplete(Result result, IOException ex, long trxnLogIndex);
}
