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

import com.google.common.base.Strings;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.util.RadixNode;
import org.apache.hadoop.ozone.util.RadixTree;
import org.apache.hadoop.utils.db.*;
import org.apache.hadoop.utils.db.Table.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PREFIX_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.PREFIX;

/**
 * Implementation of PreManager.
 */
public class PrefixManagerImpl implements PrefixManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(PrefixManagerImpl.class);

  private static final List<OzoneAcl> EMPTY_ACL_LIST = new ArrayList<>();
  private final OMMetadataManager metadataManager;

  // In-memory prefix tree to optimize ACL evaluation
  private RadixTree<OmPrefixInfo> prefixTree;

  public PrefixManagerImpl(OMMetadataManager metadataManager) {
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

  /**
   * Add acl for Ozone object. Return true if acl is added successfully else
   * false.
   *
   * @param obj Ozone object for which acl should be added.
   * @param acl ozone acl top be added.
   * @throws IOException if there is error.
   */
  @Override
  public boolean addAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    validateOzoneObj(obj);

    String prefixPath = obj.getPath();
    metadataManager.getLock().acquirePrefixLock(prefixPath);
    try {
      OmPrefixInfo prefixInfo =
          metadataManager.getPrefixTable().get(prefixPath);
      List<OzoneAcl> list = null;
      if (prefixInfo != null) {
        list = prefixInfo.getAcls();
      }

      if (list == null) {
        list = new ArrayList<>();
        list.add(acl);
      } else {
        boolean found = false;
        for (OzoneAcl a: list) {
          if (a.getName().equals(acl.getName()) &&
              a.getType() == acl.getType()) {
            found = true;
            a.getAclBitSet().or(acl.getAclBitSet());
            break;
          }
        }
        if (!found) {
          list.add(acl);
        }
      }

      OmPrefixInfo.Builder upiBuilder = OmPrefixInfo.newBuilder();
      upiBuilder.setName(prefixPath).setAcls(list);
      if (prefixInfo != null && prefixInfo.getMetadata() != null) {
        upiBuilder.addAllMetadata(prefixInfo.getMetadata());
      }
      prefixInfo = upiBuilder.build();
      // Persist into prefix table first
      metadataManager.getPrefixTable().put(prefixPath, prefixInfo);
      // update the in-memory prefix tree
      prefixTree.insert(prefixPath, prefixInfo);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Add acl operation failed for prefix path:{} acl:{}",
            prefixPath, acl, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releasePrefixLock(prefixPath);
    }
    return true;
  }

  /**
   * Remove acl for Ozone object. Return true if acl is removed successfully
   * else false.
   *
   * @param obj Ozone object.
   * @param acl Ozone acl to be removed.
   * @throws IOException if there is error.
   */
  @Override
  public boolean removeAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    validateOzoneObj(obj);
    String prefixPath = obj.getPath();
    metadataManager.getLock().acquirePrefixLock(prefixPath);
    try {
      OmPrefixInfo prefixInfo =
          metadataManager.getPrefixTable().get(prefixPath);
      List<OzoneAcl> list = null;
      if (prefixInfo != null) {
        list = prefixInfo.getAcls();
      }

      if (list == null) {
        LOG.debug("acl {} does not exist for prefix path {}", acl, prefixPath);
        return false;
      }

      boolean found = false;
      for (OzoneAcl a: list) {
        if (a.getName().equals(acl.getName())
            && a.getType() == acl.getType()) {
          found = true;
          a.getAclBitSet().andNot(acl.getAclBitSet());
          if (a.getAclBitSet().isEmpty()) {
            list.remove(a);
          }
          break;
        }
      }
      if (!found) {
        LOG.debug("acl {} does not exist for prefix path {}", acl, prefixPath);
        return false;
      }

      if (!list.isEmpty()) {
        OmPrefixInfo.Builder upiBuilder = OmPrefixInfo.newBuilder();
        upiBuilder.setName(prefixPath).setAcls(list);
        if (prefixInfo != null && prefixInfo.getMetadata() != null) {
          upiBuilder.addAllMetadata(prefixInfo.getMetadata());
        }
        prefixInfo = upiBuilder.build();
        metadataManager.getPrefixTable().put(prefixPath, prefixInfo);
        prefixTree.insert(prefixPath, prefixInfo);
      } else {
        // Remove prefix entry in table and prefix tree if the # of acls is 0
        metadataManager.getPrefixTable().delete(prefixPath);
        prefixTree.removePrefixPath(prefixPath);
      }

    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Remove prefix acl operation failed for prefix path:{}" +
            " acl:{}", prefixPath, acl, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releasePrefixLock(prefixPath);
    }
    return true;
  }

  /**
   * Acls to be set for given Ozone object. This operations reset ACL for given
   * object to list of ACLs provided in argument.
   *
   * @param obj Ozone object.
   * @param acls List of acls.
   * @throws IOException if there is error.
   */
  @Override
  public boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) throws IOException {
    validateOzoneObj(obj);
    String prefixPath = obj.getPath();
    metadataManager.getLock().acquirePrefixLock(prefixPath);
    try {
      OmPrefixInfo prefixInfo =
          metadataManager.getPrefixTable().get(prefixPath);
      OmPrefixInfo.Builder upiBuilder = OmPrefixInfo.newBuilder();
      upiBuilder.setName(prefixPath).setAcls(acls);
      if (prefixInfo != null && prefixInfo.getMetadata() != null) {
        upiBuilder.addAllMetadata(prefixInfo.getMetadata());
      }
      prefixInfo = upiBuilder.build();
      prefixTree.insert(prefixPath, prefixInfo);
      metadataManager.getPrefixTable().put(prefixPath, prefixInfo);
    } catch (IOException ex) {
      if (!(ex instanceof OMException)) {
        LOG.error("Set prefix acl operation failed for prefix path:{} acls:{}",
            prefixPath, acls, ex);
      }
      throw ex;
    } finally {
      metadataManager.getLock().releasePrefixLock(prefixPath);
    }
    return true;
  }

  /**
   * Returns list of ACLs for given Ozone object.
   *
   * @param obj Ozone object.
   * @throws IOException if there is error.
   */
  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    validateOzoneObj(obj);
    String prefixPath = obj.getPath();
    metadataManager.getLock().acquirePrefixLock(prefixPath);
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
      metadataManager.getLock().releasePrefixLock(prefixPath);
    }
    return EMPTY_ACL_LIST;
  }

  @Override
  public List<OmPrefixInfo> getLongestPrefixPath(String path) {
    String prefixPath = prefixTree.getLongestPrefix(path);
    metadataManager.getLock().acquirePrefixLock(prefixPath);
    try {
      return prefixTree.getLongestPrefixPath(prefixPath).stream()
          .map(c -> c.getValue()).collect(Collectors.toList());
    } finally {
      metadataManager.getLock().releasePrefixLock(prefixPath);
    }
  }

  /**
   * Helper method to validate ozone object.
   * @param obj
   * */
  private void validateOzoneObj(OzoneObj obj) throws OMException {
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
      throw new OMException("Invalid prefix name: " + prefixName,
          PREFIX_NOT_FOUND);
    }
  }
}
