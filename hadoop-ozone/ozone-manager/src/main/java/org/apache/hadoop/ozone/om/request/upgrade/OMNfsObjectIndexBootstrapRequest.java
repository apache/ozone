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

package org.apache.hadoop.ozone.om.request.upgrade;

import static org.apache.hadoop.ozone.OzoneConsts.NFS_OBJECT_INDEX_PROGRESS_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.NFS_OBJECT_INDEX_READY_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.ACCESS_DENIED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import com.google.common.primitives.UnsignedBytes;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsObjectInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.upgrade.OMNfsObjectIndexBootstrapResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BootstrapNfsObjectIndexRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BootstrapNfsObjectIndexRequest.Phase;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BootstrapNfsObjectIndexResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedNfsObjectInfo.Kind;

/** Revalidates a bounded, single-bucket scan batch against the current canonical namespace. */
public class OMNfsObjectIndexBootstrapRequest extends OMClientRequest {
  public static final int MAX_OBJECTS = 500;

  public OMNfsObjectIndexBootstrapRequest(OMRequest request) {
    super(request);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMRequest request = super.preExecute(ozoneManager);
    throw new OMException(request.getCmdType() + " is an internal OM operation", ACCESS_DENIED);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    OMMetadataManager manager = ozoneManager.getMetadataManager();
    OMResponse.Builder response = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    BootstrapNfsObjectIndexRequest request = getOmRequest().getBootstrapNfsObjectIndexRequest();
    boolean locked = false;
    try {
      if (!manager.isNfsObjectIndexEnabled()) {
        throw new OMException("NFS index layout is not finalized", NOT_SUPPORTED_OPERATION);
      }
      validateBatch(manager, request);
      Map<String, OmNfsObjectInfo> objects = new LinkedHashMap<>();
      if (request.getObjectsCount() > 0) {
        locked = manager.getLock().acquireWriteLock(BUCKET_LOCK,
            request.getVolumeName(), request.getBucketName()).isLockAcquired();
        if (!locked) {
          throw new OMException("Cannot lock bootstrap bucket", INVALID_REQUEST);
        }
        collectCurrentObjects(manager, request, objects);
      }
      String progress = request.getFinished()
          ? (request.getPhase() == Phase.DIRECTORIES ? "2:" : "DONE")
          : request.getPhase().getNumber() + ":" + request.getNextCursor();
      boolean ready = request.getFinished() && request.getPhase() == Phase.FILES;
      objects.forEach((key, value) -> manager.getNfsObjectTable().addCacheEntry(key, value, context.getIndex()));
      manager.getMetaTable().addCacheEntry(NFS_OBJECT_INDEX_PROGRESS_KEY, progress, context.getIndex());
      if (ready) {
        manager.getMetaTable().addCacheEntry(NFS_OBJECT_INDEX_READY_KEY,
            Integer.toString(OMLayoutFeature.NFS_OBJECT_INDEX.serialize()), context.getIndex());
      }
      response.setBootstrapNfsObjectIndexResponse(BootstrapNfsObjectIndexResponse.newBuilder());
      return new OMNfsObjectIndexBootstrapResponse(response.build(), objects, progress, ready);
    } catch (IOException | IllegalArgumentException ex) {
      return new OMNfsObjectIndexBootstrapResponse(createErrorOMResponse(response, ex));
    } finally {
      if (locked) {
        manager.getLock().releaseWriteLock(BUCKET_LOCK, request.getVolumeName(), request.getBucketName());
      }
    }
  }

  private void validateBatch(OMMetadataManager manager, BootstrapNfsObjectIndexRequest request) throws IOException {
    String current = manager.getMetaTable().get(NFS_OBJECT_INDEX_PROGRESS_KEY);
    String expected = request.getPhase().getNumber() + ":" + request.getExpectedCursor();
    if (request.getInitialize()) {
      if (current != null || request.getPhase() != Phase.DIRECTORIES || !request.getExpectedCursor().isEmpty()
          || !request.getNextCursor().isEmpty() || request.getFinished() || request.getObjectsCount() != 0) {
        throw new OMException("Invalid NFS bootstrap initialization", INVALID_REQUEST);
      }
      return;
    }
    if (current == null || !current.equals(expected)) {
      throw new OMException("NFS bootstrap progress changed", INVALID_REQUEST);
    }
    if (request.getObjectsCount() > MAX_OBJECTS
        || compareKeys(request.getNextCursor(), request.getExpectedCursor()) < 0
        || (!request.getFinished() && request.getNextCursor().equals(request.getExpectedCursor()))) {
      throw new OMException("Invalid NFS bootstrap batch bounds", INVALID_REQUEST);
    }
    String previous = request.getExpectedCursor();
    String prefix = manager.getNfsObjectKey(request.getVolumeID(), request.getBucketID(), 0);
    prefix = prefix.substring(0, prefix.lastIndexOf('/') + 1);
    for (BootstrapNfsObjectIndexRequest.Object object : request.getObjectsList()) {
      if (request.getVolumeName().isEmpty() || request.getBucketName().isEmpty()
          || !object.getDbKey().startsWith(prefix) || compareKeys(object.getDbKey(), previous) <= 0
          || compareKeys(object.getDbKey(), request.getNextCursor()) > 0) {
        throw new OMException("Invalid NFS bootstrap object ordering or bucket", INVALID_REQUEST);
      }
      previous = object.getDbKey();
    }
  }

  private static int compareKeys(String left, String right) throws IOException {
    return UnsignedBytes.lexicographicalComparator().compare(
        StringCodec.get().toPersistedFormat(left), StringCodec.get().toPersistedFormat(right));
  }

  private void collectCurrentObjects(OMMetadataManager manager, BootstrapNfsObjectIndexRequest request,
      Map<String, OmNfsObjectInfo> objects) throws IOException {
    OmVolumeArgs volume = manager.getVolumeTable().get(manager.getVolumeKey(request.getVolumeName()));
    OmBucketInfo bucket = manager.getBucketTable().get(
        manager.getBucketKey(request.getVolumeName(), request.getBucketName()));
    if (volume == null || bucket == null || volume.getObjectID() != request.getVolumeID()
        || bucket.getObjectID() != request.getBucketID()
        || bucket.getBucketLayout() != BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      return;
    }
    for (BootstrapNfsObjectIndexRequest.Object object : request.getObjectsList()) {
      OmNfsObjectInfo pointer = null;
      if (request.getPhase() == Phase.DIRECTORIES) {
        OmDirectoryInfo directory = manager.getDirectoryTable().get(object.getDbKey());
        if (directory != null && directory.getObjectID() == object.getObjectID()) {
          String key = manager.getOzonePathKey(request.getVolumeID(), request.getBucketID(), directory);
          if (!object.getDbKey().equals(key)) {
            throw new OMException("Canonical directory key does not match bootstrap object", INVALID_REQUEST);
          }
          pointer = new OmNfsObjectInfo(Kind.DIRECTORY, directory.getParentObjectID(), directory.getName());
        }
      } else {
        OmKeyInfo file = manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).get(object.getDbKey());
        if (file != null && file.getObjectID() == object.getObjectID()) {
          String key = manager.getOzonePathKey(request.getVolumeID(), request.getBucketID(),
              file.getParentObjectID(), file.getFileName());
          if (!object.getDbKey().equals(key) || !request.getVolumeName().equals(file.getVolumeName())
              || !request.getBucketName().equals(file.getBucketName())) {
            throw new OMException("Canonical file key does not match bootstrap object", INVALID_REQUEST);
          }
          pointer = new OmNfsObjectInfo(Kind.FILE, file.getParentObjectID(), file.getFileName());
        }
      }
      if (pointer != null) {
        String key = manager.getNfsObjectKey(request.getVolumeID(), request.getBucketID(), object.getObjectID());
        objects.put(key, pointer);
      }
    }
  }
}
