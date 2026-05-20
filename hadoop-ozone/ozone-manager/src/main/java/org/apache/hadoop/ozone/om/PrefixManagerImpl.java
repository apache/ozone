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

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_PATH_IN_ACL_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PREFIX_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.PREFIX_LOCK;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.PREFIX;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.ozone.util.RadixNode;
import org.apache.hadoop.ozone.util.RadixTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of PrefixManager.
 */
public class PrefixManagerImpl implements PrefixManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(PrefixManagerImpl.class);

  private static final List<OzoneAcl> EMPTY_ACL_LIST = new ArrayList<>();
  private final OzoneManager ozoneManager;
  private final OMMetadataManager metadataManager;

  // In-memory prefix tree to optimize ACL evaluation
  private RadixTree<OmPrefixInfo> prefixTree;

  // Ratis is disabled for snapshots
  private final boolean isRatisEnabled;

  public PrefixManagerImpl(OzoneManager ozoneManager, OMMetadataManager metadataManager,
      boolean isRatisEnabled) {
    this.isRatisEnabled = isRatisEnabled;
    this.ozoneManager = ozoneManager;
    this.metadataManager = metadataManager;
    loadPrefixTree();
  }

  private void loadPrefixTree() {
    prefixTree = new RadixTree<>();
    try (TableIterator<String, ? extends
        KeyValue<String, OmPrefixInfo>> iterator =
             getMetadataManager().getPrefixTable().iterator()) {
      iterator.seekToFirst();
      while (iterator.hasNext()) {
        KeyValue<String, OmPrefixInfo> kv = iterator.next();
        prefixTree.insert(kv.getKey(), kv.getValue());
      }
    } catch (IOException ex) {
      LOG.error("Fail to load prefix tree");
    }
  }

  @Override
  public OMMetadataManager getMetadataManager() {
    return metadataManager;
  }

  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    validateOzoneObj(obj);
    OzoneObj resolvedObj = getResolvedPrefixObj(obj);
    String prefixPath = resolvedObj.getPath();
    metadataManager.getLock().acquireReadLock(PREFIX_LOCK, prefixPath);
    try {
      String longestPrefix = prefixTree.getLongestPrefix(prefixPath);
      if (prefixPath.equals(longestPrefix)) {
        RadixNode<OmPrefixInfo> lastNode =
            prefixTree.getLastNodeInPrefixPath(prefixPath);
        if (lastNode != null && lastNode.getValue() != null) {
          return lastNode.getValue().getAcls();
        }
      }
    } finally {
      metadataManager.getLock().releaseReadLock(PREFIX_LOCK, prefixPath);
    }
    return EMPTY_ACL_LIST;
  }

  @VisibleForTesting
  public OmPrefixInfo getPrefixInfo(OzoneObj obj) throws IOException {
    validateOzoneObj(obj);
    String prefixPath = obj.getPath();
    metadataManager.getLock().acquireReadLock(PREFIX_LOCK, prefixPath);
    try {
      String longestPrefix = prefixTree.getLongestPrefix(prefixPath);
      if (prefixPath.equals(longestPrefix)) {
        RadixNode<OmPrefixInfo> lastNode =
            prefixTree.getLastNodeInPrefixPath(prefixPath);
        if (lastNode != null && lastNode.getValue() != null) {
          return lastNode.getValue();
        }
      }
    } finally {
      metadataManager.getLock().releaseReadLock(PREFIX_LOCK, prefixPath);
    }
    return null;
  }

  /**
   * Check access for given ozoneObject.
   *
   * @param ozObject object for which access needs to be checked.
   * @param context Context object encapsulating all user related information.
   * @return true if user has access else false.
   */
  @Override
  public boolean checkAccess(OzoneObj ozObject, RequestContext context)
      throws OMException {
    Objects.requireNonNull(ozObject);
    Objects.requireNonNull(context);

    OzoneObj resolvedObj;
    try {
      resolvedObj = getResolvedPrefixObj(ozObject);
    } catch (IOException e) {
      throw new OMException("Failed to resolveBucketLink:", e, INTERNAL_ERROR);
    }

    String prefixPath = resolvedObj.getPath();
    metadataManager.getLock().acquireReadLock(PREFIX_LOCK, prefixPath);
    try {
      String longestPrefix = prefixTree.getLongestPrefix(prefixPath);
      if (prefixPath.equals(longestPrefix)) {
        RadixNode<OmPrefixInfo> lastNode =
            prefixTree.getLastNodeInPrefixPath(prefixPath);
        if (lastNode != null && lastNode.getValue() != null) {
          boolean hasAccess = OzoneAclUtil.checkAclRights(lastNode.getValue().
              getAcls(), context);
          if (LOG.isDebugEnabled()) {
            LOG.debug("user:{} has access rights for ozObj:{} ::{} ",
                context.getClientUgi(), ozObject, hasAccess);
          }
          return hasAccess;
        }
      }
    } finally {
      metadataManager.getLock().releaseReadLock(PREFIX_LOCK, prefixPath);
    }
    return true;
  }

  @Override
  public List<OmPrefixInfo> getLongestPrefixPath(String path) {
    String prefixPath = prefixTree.getLongestPrefix(path);
    metadataManager.getLock().acquireReadLock(PREFIX_LOCK, prefixPath);
    try {
      return getLongestPrefixPathHelper(prefixPath);
    } finally {
      metadataManager.getLock().releaseReadLock(PREFIX_LOCK, prefixPath);
    }
  }

  /**
   * Get longest prefix path assuming caller take prefix lock.
   * @param prefixPath
   * @return list of prefix info.
   */
  private List<OmPrefixInfo> getLongestPrefixPathHelper(String prefixPath) {
    return prefixTree.getLongestPrefixPath(prefixPath).stream()
          .map(c -> c.getValue()).collect(Collectors.toList());
  }

  /**
   * Helper method to validate ozone object.
   * @param obj
   * */
  public void validateOzoneObj(OzoneObj obj) throws OMException {
    Objects.requireNonNull(obj);

    if (!obj.getResourceType().equals(PREFIX)) {
      throw new IllegalArgumentException("Unexpected argument passed to " +
          "PrefixManager. OzoneObj type:" + obj.getResourceType());
    }
    String volume = obj.getVolumeName();
    String bucket = obj.getBucketName();
    String prefixName = obj.getPrefixName();

    if (Strings.isNullOrEmpty(volume)) {
      throw new OMException("Volume name is required.", VOLUME_NOT_FOUND);
    }
    if (Strings.isNullOrEmpty(bucket)) {
      throw new OMException("Bucket name is required.", BUCKET_NOT_FOUND);
    }
    if (Strings.isNullOrEmpty(prefixName)) {
      throw new OMException("Prefix name is required.", PREFIX_NOT_FOUND);
    }
    if (!prefixName.endsWith("/")) {
      throw new OMException("Missing trailing slash '/' in prefix name: " + prefixName,
          INVALID_PATH_IN_ACL_REQUEST);
    }
  }

  public OMPrefixAclOpResult addAcl(OzoneObj ozoneObj, OzoneAcl ozoneAcl,
      OmPrefixInfo prefixInfo, long transactionLogIndex) throws IOException {
    // No explicit prefix create API, both add/set Acl can get new prefix
    // created. When new prefix is created, it should inherit parent prefix
    // or bucket default ACLs.
    boolean newPrefix = prefixInfo == null;
    OmPrefixInfo.Builder prefixInfoBuilder = newPrefix
        ? OmPrefixInfo.newBuilder().setName(ozoneObj.getPath())
        : prefixInfo.toBuilder();

    if (newPrefix && transactionLogIndex > 0) {
      prefixInfoBuilder.setObjectID(OmUtils.getObjectIdFromTxId(
          metadataManager.getOmEpoch(), transactionLogIndex));
      prefixInfoBuilder.setUpdateID(transactionLogIndex);
    }

    // Update the in-memory prefix tree regardless whether the ACL is changed.
    // Under OM HA, update ID of the prefix info is updated for every request.
    if (newPrefix) {
      List<OzoneAcl> inheritedAcls = new ArrayList<>();
      inheritParentAcl(ozoneObj, inheritedAcls);
      prefixInfoBuilder.addAcls(inheritedAcls);
    }
    prefixInfoBuilder.addAcl(ozoneAcl);
    boolean changed = prefixInfoBuilder.isAclsChanged();

    OmPrefixInfo updatedPrefixInfo = prefixInfoBuilder.build();
    // update the in-memory prefix tree
    prefixTree.insert(ozoneObj.getPath(), updatedPrefixInfo);

    if (!isRatisEnabled) {
      metadataManager.getPrefixTable().put(ozoneObj.getPath(), updatedPrefixInfo);
    }
    return new OMPrefixAclOpResult(updatedPrefixInfo, changed);
  }

  public OMPrefixAclOpResult removeAcl(OzoneObj ozoneObj, OzoneAcl ozoneAcl,
      OmPrefixInfo prefixInfo) throws IOException {
    if (prefixInfo == null) {
      return new OMPrefixAclOpResult(null, false);
    }

    OmPrefixInfo.Builder prefixInfoBuilder = prefixInfo.toBuilder();
    prefixInfoBuilder.removeAcl(ozoneAcl);
    boolean removed = prefixInfoBuilder.isAclsChanged();

    OmPrefixInfo updatedPrefixInfo = removed
        ? prefixInfoBuilder.build()
        : prefixInfo;

    // Update in-memory prefix tree regardless whether the ACL is changed.
    // Under OM HA, update ID of the prefix info is updated for every request.
    if (removed && updatedPrefixInfo.getAcls().isEmpty()) {
      prefixTree.removePrefixPath(ozoneObj.getPath());
      if (!isRatisEnabled) {
        metadataManager.getPrefixTable().delete(ozoneObj.getPath());
      }
    } else {
      prefixTree.insert(ozoneObj.getPath(), updatedPrefixInfo);
      if (!isRatisEnabled) {
        metadataManager.getPrefixTable().put(ozoneObj.getPath(), updatedPrefixInfo);
      }
    }
    return new OMPrefixAclOpResult(updatedPrefixInfo, removed);
  }

  private void inheritParentAcl(OzoneObj ozoneObj, List<OzoneAcl> aclsToBeSet)
      throws IOException {
    // Inherit DEFAULT acls from prefix.
    boolean prefixParentFound = false;
    List<OmPrefixInfo> prefixList = getLongestPrefixPathHelper(
        prefixTree.getLongestPrefix(ozoneObj.getPath()));

    if (!prefixList.isEmpty()) {
      // Add all acls from direct parent to key.
      OmPrefixInfo parentPrefixInfo = prefixList.get(prefixList.size() - 1);
      if (parentPrefixInfo != null) {
        prefixParentFound = OzoneAclUtil.inheritDefaultAcls(
            aclsToBeSet, parentPrefixInfo.getAcls(), ACCESS);
      }
    }

    // If no parent prefix is found inherit DEFAULT acls from bucket.
    if (!prefixParentFound) {
      String bucketKey = metadataManager.getBucketKey(ozoneObj
          .getVolumeName(), ozoneObj.getBucketName());
      OmBucketInfo bucketInfo = metadataManager.getBucketTable().
          get(bucketKey);
      if (bucketInfo != null) {
        OzoneAclUtil.inheritDefaultAcls(aclsToBeSet, bucketInfo.getAcls(), ACCESS);
      }
    }
  }

  public OMPrefixAclOpResult setAcl(OzoneObj ozoneObj, List<OzoneAcl> ozoneAcls,
      OmPrefixInfo prefixInfo, long transactionLogIndex) throws IOException {
    boolean newPrefix = prefixInfo == null;
    OmPrefixInfo.Builder prefixInfoBuilder = newPrefix
        ? OmPrefixInfo.newBuilder().setName(ozoneObj.getPath())
        : prefixInfo.toBuilder();

    if (newPrefix && transactionLogIndex > 0) {
      prefixInfoBuilder.setObjectID(OmUtils.getObjectIdFromTxId(
          metadataManager.getOmEpoch(), transactionLogIndex));
      prefixInfoBuilder.setUpdateID(transactionLogIndex);
    }

    List<OzoneAcl> aclsToSet = new ArrayList<>();
    OzoneAclUtil.setAcl(aclsToSet, ozoneAcls);
    if (newPrefix) {
      inheritParentAcl(ozoneObj, aclsToSet);
    }
    prefixInfoBuilder.setAcls(aclsToSet);
    // For new prefix, creation itself is considered a change regardless of ACL content
    boolean changed = newPrefix || prefixInfoBuilder.isAclsChanged();

    OmPrefixInfo updatedPrefixInfo = prefixInfoBuilder.build();
    prefixTree.insert(ozoneObj.getPath(), updatedPrefixInfo);
    if (!isRatisEnabled) {
      metadataManager.getPrefixTable().put(ozoneObj.getPath(), updatedPrefixInfo);
    }
    return new OMPrefixAclOpResult(updatedPrefixInfo, changed);
  }

  /**
   * Get the resolved prefix object to handle prefix that is under a link bucket.
   * @param obj prefix object
   * @return the resolved prefix object if the object belongs under a link bucket.
   * Otherwise, return the same prefix object.
   * @throws IOException Exception thrown when resolving the bucket link.
   */
  public OzoneObj getResolvedPrefixObj(OzoneObj obj) throws IOException {
    if (StringUtils.isEmpty(obj.getVolumeName()) || StringUtils.isEmpty(obj.getBucketName())) {
      return obj;
    }

    ResolvedBucket resolvedBucket = ozoneManager.resolveBucketLink(
        Pair.of(obj.getVolumeName(), obj.getBucketName()));
    return resolvedBucket.update(obj);
  }

  /**
   * Result of the prefix acl operation.
   */
  public static class OMPrefixAclOpResult {
    /** The updated prefix info after applying the prefix acl operation. */
    private final OmPrefixInfo omPrefixInfo;
    /** Operation result, success if the underlying ACL is changed, false otherwise. */
    private final boolean operationsResult;

    public OMPrefixAclOpResult(OmPrefixInfo omPrefixInfo,
        boolean operationsResult) {
      this.omPrefixInfo = omPrefixInfo;
      this.operationsResult = operationsResult;
    }

    public OmPrefixInfo getOmPrefixInfo() {
      return omPrefixInfo;
    }

    public boolean isSuccess() {
      return operationsResult;
    }
  }
}
