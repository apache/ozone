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

package org.apache.hadoop.ozone.om.request.volume.acl;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.VOLUME_LOCK;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.ratis.util.function.CheckedBiConsumer;

/**
 * Base class for OMVolumeAcl Request.
 */
public abstract class OMVolumeAclRequest extends OMVolumeRequest {

  private final VolumeAclOp omVolumeAclOp;

  OMVolumeAclRequest(OzoneManagerProtocolProtos.OMRequest omRequest,
      VolumeAclOp aclOp) {
    super(omRequest);
    omVolumeAclOp = aclOp;
  }

  @Override
  public OzoneManagerProtocolProtos.OMRequest preExecute(OzoneManager ozoneManager)
      throws IOException {
    OzoneManagerProtocolProtos.OMRequest omRequest = super.preExecute(ozoneManager);
    
    // ACL check during preExecute
    if (ozoneManager.getAclsEnabled()) {
      String volume = getVolumeName();
      try {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
            volume, null, null);
      } catch (IOException ex) {
        // Ensure audit log captures preExecute failures
        Map<String, String> auditMap = new LinkedHashMap<>();
        auditMap.put(OzoneConsts.VOLUME, volume);
        List<OzoneAcl> acls = getAcls();
        if (acls != null) {
          auditMap.put(OzoneConsts.ACL, acls.toString());
        }
        // Determine which action based on request type
        OMAction action = OMAction.SET_ACL;
        if (omRequest.hasAddAclRequest()) {
          action = OMAction.ADD_ACL;
        } else if (omRequest.hasRemoveAclRequest()) {
          action = OMAction.REMOVE_ACL;
        }
        markForAudit(ozoneManager.getAuditLogger(),
            buildAuditMessage(action, auditMap, ex,
                omRequest.getUserInfo()));
        throw ex;
      }
    }
    
    return omRequest;
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();
    // protobuf guarantees volume and acls are non-null.
    String volume = getVolumeName();
    List<OzoneAcl> ozoneAcls = getAcls();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumVolumeUpdates();

    OMResponse.Builder omResponse = onInit();
    OMClientResponse omClientResponse = null;
    Exception exception = null;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean lockAcquired = false;
    Result result;
    try {
      mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, volume));
      lockAcquired = getOmLockDetails().isLockAcquired();
      OmVolumeArgs omVolumeArgs = getVolumeInfo(omMetadataManager, volume);

      // result is false upon add existing acl or remove non-existing acl
      boolean applyAcl = true;
      try {
        omVolumeAclOp.accept(ozoneAcls, omVolumeArgs);
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
        } else if (getOmRequest().getSetAclRequest().hasObj()) {
          modificationTime = getOmRequest().getSetAclRequest()
              .getModificationTime();
        } else if (getOmRequest().getRemoveAclRequest().hasObj()) {
          modificationTime = getOmRequest().getRemoveAclRequest()
              .getModificationTime();
        }
        omVolumeArgs.setModificationTime(modificationTime);

        omVolumeArgs.setUpdateID(trxnLogIndex);

        // update cache.
        omMetadataManager.getVolumeTable().addCacheEntry(
            new CacheKey<>(omMetadataManager.getVolumeKey(volume)),
            CacheValue.get(trxnLogIndex, omVolumeArgs));
      }

      omClientResponse = onSuccess(omResponse, omVolumeArgs, applyAcl);
      result = Result.SUCCESS;
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omMetrics.incNumVolumeUpdateFails();
      omClientResponse = onFailure(omResponse, exception);
    } finally {
      if (lockAcquired) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(VOLUME_LOCK, volume));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    OzoneObj obj = getObject();
    Map<String, String> auditMap = obj.toAuditMap();
    if (ozoneAcls != null) {
      auditMap.put(OzoneConsts.ACL, ozoneAcls.toString());
    }
    onComplete(result, exception, trxnLogIndex, ozoneManager.getAuditLogger(),
        auditMap);

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

  /**
   * Get the Volume object Info from the request.
   * @return OzoneObjInfo
   */
  abstract OzoneObj getObject();

  // TODO: Finer grain metrics can be moved to these callbacks. They can also
  // be abstracted into separate interfaces in future.
  /**
   * Get the initial OM response builder with lock.
   * @return om response builder.
   */
  abstract OMResponse.Builder onInit();

  /**
   * Get the OM client response on success case with lock.
   */
  abstract OMClientResponse onSuccess(
      OMResponse.Builder omResponse, OmVolumeArgs omVolumeArgs,
      boolean aclApplied);

  /**
   * Get the OM client response on failure case with lock.
   */
  abstract OMClientResponse onFailure(OMResponse.Builder omResponse,
      Exception ex);

  /**
   * Completion hook for final processing before return without lock.
   * Usually used for logging without lock.
   */
  abstract void onComplete(Result result, Exception ex, long trxnLogIndex,
      AuditLogger auditLogger, Map<String, String> auditMap);

  /**
   * Volume ACL operation.
   */
  public interface VolumeAclOp extends
      CheckedBiConsumer<List<OzoneAcl>, OmVolumeArgs, IOException> {
    // just a shortcut to avoid having to repeat long list of generic parameters
  }
}
